%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Buffering strategy is a behaviour for different approaches to buffering
%%%   requests in excess of the number of Cassandra Connections.

%%%   The current release provides the following buffering strategy
%%%   implementations:
%%%
%%%     elysium_bs_serial:
%%%        A connection queue and a pending request queue, each
%%%        implemented as a single gen_server with an erlang queue
%%%        as the internal state. Loss of a gen_server loses all
%%%        queue entries.
%%%
%%%     elysium_bs_parallel:
%%%        A connection queue and a pending request queue, each
%%%        implemented as a FIFO ets_buffer. The ets_buffers
%%%        survive all failures up to the elysium_sup itself.
%%%
%%%        As of Nov 13, 2014 this approach has concurrency
%%%        issues related to ets_missing_data errors.
%%%
%%% @since 0.1.7
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_buffering_strategy).
-author('jay@duomark.com').

-include("elysium_types.hrl").
-include("elysium_audit_types.hrl").

%% External API supplied for behaviour implementations to share.
-export([
         status/1,
         queue_status/2,
         queue_status_reset/2,

         create_connection/3,
         checkin_connection/4,
         checkout_connection/1,
         pend_request/2,

         decay_causes_death/2,
         decay_connection/3,
         report_available_resources/3,
         handle_pending_request/7
        ]).


%%%-----------------------------------------------------------------------
%%% Callbacks that behaviour implementers must provide
%%%-----------------------------------------------------------------------

-callback idle_connection_count(connection_queue_name()) -> max_connections().
-callback checkin_connection(connection_queue_name(), cassandra_connection()) -> max_connections().
-callback checkout_connection(connection_queue_name())
                      -> connection_baton() | none_available | Error when Error::any().
-callback checkout_connection_error(config_type(), buffering_strategy_module(), connection_queue_name(), Error)
                      -> retry | Error when Error::any().

-callback pending_request_count(requests_queue_name()) -> pending_count().
-callback checkin_pending_request(requests_queue_name(), reference(), erlang:timestamp())
                      -> pending_count().
-callback checkout_pending_request(requests_queue_name())
                      -> pending_request_baton() | none_available | Error when Error::any().
-callback checkout_pending_error(config_type(), buffering_strategy_module(), requests_queue_name(), Error)
                      -> retry | Error when Error::any().

-callback queue_status       (connection_queue_name() | requests_queue_name()) -> {status, proplists:proplist()}.
-callback queue_status_reset (connection_queue_name() | requests_queue_name()) -> {status, proplists:proplist()}.


%%%-----------------------------------------------------------------------
%%% Buffering strategy provided functions
%%%-----------------------------------------------------------------------

-spec create_connection(config_type(), cassandra_node(), connection_id())
                        -> pending_checkin().

-spec checkin_connection(config_type(), cassandra_node(), connection_id(), Is_New_Connection::boolean())
                        -> pending_checkin().

-spec checkout_connection(config_type()) -> cassandra_connection() | none_available.

-spec pend_request(config_type(), query_request()) -> query_result().

-spec decay_causes_death (config_type(), connection_id()) -> boolean().
-spec decay_connection   (config_type(), buffering_strategy_module(), connection_id()) -> true.

-spec status             (config_type()) -> status_reply().
-spec queue_status       (config_type(), connection_queue_name() | requests_queue_name())
                         -> {status, proplists:proplist()}.
-spec queue_status_reset (config_type(), connection_queue_name() | requests_queue_name())
                         -> {status, proplists:proplist()}.

-spec report_available_resources(Queue, Connections, Max_Or_Timeout) -> {Queue, Connections, Max_Or_Timeout}
                                       when Queue          :: queue_name(),
                                            Connections    :: max_connections() | pending_count(),
                                            Max_Or_Timeout :: max_connections() | timeout_in_ms().

%% @doc Do any initialization before checkin of a newly created connection.
create_connection(Config, {_Ip, _Port} = Node, Connection_Id)
  when is_pid(Connection_Id) ->
    {_Buffering_Strategy, BS_Module} = elysium_connection:get_buffer_strategy_module(Config),
    elysium_buffering_audit:audit_data_init(Config, BS_Module, Connection_Id),
    checkin_connection(Config, Node, Connection_Id, true).

%% @doc
%%   Checkin a Cassandra Connection, IF there are no Pending Requests.
%%   A checkin will continue looping on the pending queue with
%%   the chance for decay on each attempt. If it decays, any
%%   newly spawned replacement is expected to check the pending
%%   queue for outstanding requests. Brand new connections are
%%   not checked for decay before first use.
%% @end
checkin_connection(Config, {_Ip, _Port} = Node, Connection_Id, Is_New_Connection)
  when is_pid(Connection_Id) ->
    Pending_Queue    = elysium_config:requests_queue_name            (Config),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module (Config),
    case is_process_alive(Connection_Id) andalso BS_Module:pending_request_count(Pending_Queue) =:= 0 of
        true  -> checkin_immediate (Config, BS_Module, Node, Connection_Id, Pending_Queue, Is_New_Connection);
        false -> checkin_pending   (Config, BS_Module, Node, Connection_Id, Pending_Queue, Is_New_Connection)
    end.

%% @doc
%%   Allocate a Cassandra Connection to the caller by popping an entry
%%   from the front of the connection queue. This function either
%%   returns a live pid(), or none_available to indicate that all
%%   connections to Cassandra are currently checked out.
%%
%%   The configuration parameter is not validated because this
%%   function should be a hotspot and we don't want it to slow
%%   down or become a concurrency bottleneck.
%% @end
checkout_connection(Config) ->
    Connection_Queue = elysium_config:session_queue_name             (Config),
    Max_Retries      = elysium_config:checkout_max_retry             (Config),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module (Config),
    fetch_pid_from_connection_queue(Config, BS_Module, Connection_Queue, Max_Retries, -1).

%% @doc
%%   Block the caller until Cassandra Connection is available
%%   to run this Pending Request. The blocking recieve loop
%%   will unblock and a spawned process will execute the request,
%%   so that the caller can still timeout if the request takes too long.
%% @end
pend_request(Config, Query_Request) ->
    Sid_Reply_Ref    = make_ref(),
    Start_Time       = os:timestamp(),
    Pending_Queue    = elysium_config:requests_queue_name            (Config),
    Reply_Timeout    = elysium_config:request_reply_timeout          (Config),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module (Config),
    wait_for_connection(Config, BS_Module, Pending_Queue, Sid_Reply_Ref,
                        Start_Time, Query_Request, Reply_Timeout).

%% @doc Get the next Cassandra Connection entry from the set of available Connections.
fetch_pid_from_connection_queue(_Config, _BS_Module, _Connection_Queue, Max_Retries, Times_Tried)
  when Times_Tried >= Max_Retries ->
    none_available;
fetch_pid_from_connection_queue( Config,  BS_Module,  Connection_Queue, Max_Retries, Times_Tried) ->
    case BS_Module:checkout_connection(Connection_Queue) of
        none_available -> none_available;

        %% Return only a live pid, otherwise get the next one.
        {_Node, Connection_Id} = Connection_Data when is_pid(Connection_Id) ->
            case is_process_alive(Connection_Id) of
                %% NOTE: we toss only MAX_CHECKOUT_RETRY dead pids
                false -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_dead),
                         fetch_pid_from_connection_queue(Config, BS_Module, Connection_Queue,
                                                         Max_Retries, Times_Tried+1);
                true  -> _ = elysium_buffering_audit:audit_data_checkout(Config, BS_Module, Connection_Id),
                         Connection_Data
            end;

        Error ->
            case BS_Module:checkout_connection_error(Config, BS_Module, Connection_Queue, Error) of
                retry -> fetch_pid_from_connection_queue(Config, BS_Module, Connection_Queue,
                                                         Max_Retries, Times_Tried+1);
                Error -> Error
            end
    end.

wait_for_connection(Config, BS_Module, Pending_Queue, Sid_Reply_Ref, Start_Time, Query_Request, Reply_Timeout) ->
    Connection_Queue = elysium_config:session_queue_name(Config),
    case fetch_pid_from_connection_queue(Config, BS_Module, Connection_Queue, 1, 0) of

        %% A free cassandra connection showed up since we first checked...
        {Node, Connection_Id} when is_pid(Connection_Id) ->
            case is_process_alive(Connection_Id) of
                true  -> handle_pending_request(Config, BS_Module, 0, Reply_Timeout, Node,
                                                Connection_Id, Query_Request);
                false -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_dead),
                         wait_for_connection(Config, BS_Module, Pending_Queue, Sid_Reply_Ref,
                                             Start_Time, Query_Request, Reply_Timeout)
            end;

        %% None are still available, queue the request and wait for one to free up.
        _None_Available ->
            _Pending_Count = BS_Module:checkin_pending_request(Pending_Queue, Sid_Reply_Ref, Start_Time),
            wait_for_connection_loop(Config, BS_Module, Pending_Queue, Sid_Reply_Ref,
                                     Start_Time, Query_Request, Reply_Timeout)
    end.

wait_for_connection_loop(Config, BS_Module, Pending_Queue, Sid_Reply_Ref,
                         Start_Time, Query_Request, Reply_Timeout) ->
    receive
        %% A live Cassandra Connection is now available to make the request...
        {sid, Sid_Reply_Ref, Node, Connection_Id, Pending_Queue, Is_New_Connection} ->
            Elapsed_Time = timer:now_diff(os:timestamp(), Start_Time),
            case {Elapsed_Time >= Reply_Timeout * 1000, is_process_alive(Connection_Id)} of

                %% Alas, we timed out waiting...
                {true,  true}  -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_timeouts),
                                  _ = checkin_connection(Config, Node, Connection_Id, Is_New_Connection),
                                  {wait_for_session_timeout, Reply_Timeout};
                {true,  false} -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_dead),
                                  _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_timeouts),
                                  {wait_for_session_timeout, Reply_Timeout};

                %% Dead session, loop waiting for another (hopefully live) connection to free up...
                {false, false} -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_dead),
                                  New_Timeout = Reply_Timeout - (Elapsed_Time div 1000),
                                  wait_for_connection(Config, BS_Module, Pending_Queue, Sid_Reply_Ref,
                                                      Start_Time, Query_Request, New_Timeout);

                %% Get some results while we still have time!
                {false, true}  ->
                    handle_pending_request(Config, BS_Module, Elapsed_Time, Reply_Timeout,
                                           Node, Connection_Id, Query_Request)
            end;

        %% Previous timed out request sent a Connection_Id late, check it in and wait for our expected one.
        {sid, _, Node, Connection_Id, Pending_Queue, Is_New_Connection} ->
            _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_wrong),
            _ = case is_process_alive(Connection_Id) of
                    false -> elysium_buffering_audit:audit_count(Config, BS_Module, session_dead);
                    true  -> checkin_immediate(Config, BS_Module, Node, Connection_Id,
                                               Pending_Queue, Is_New_Connection)
                end,
            Elapsed_Time = timer:now_diff(os:timestamp(), Start_Time),
            case Elapsed_Time >= Reply_Timeout * 1000 of
                true -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_timeouts),
                        {wait_for_session_timeout, Reply_Timeout};
                false -> New_Timeout = Reply_Timeout - (Elapsed_Time div 1000),
                         wait_for_connection_loop(Config, BS_Module, Pending_Queue, Sid_Reply_Ref,
                                                  Start_Time, Query_Request, New_Timeout)
            end

        %% Any other messages are intended for the blocked caller, leave them in the message queue.

    after Reply_Timeout ->
            %% Handle race condition messaging vs timeout waiting for message.
            erlang:yield(),
            _ = receive
                    {sid, Sid_Reply_Ref, Node, Connection_Id, Pending_Queue, Is_New_Connection} ->
                        case is_process_alive(Connection_Id) of
                            false -> elysium_buffering_audit:audit_count(Config, BS_Module, session_dead);
                            true  -> checkin_connection(Config, Node, Connection_Id, Is_New_Connection)
                        end
                after 0 -> no_msgs
                end,
            _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_timeouts),
            {wait_for_session_timeout, Reply_Timeout}
    end.

%% @doc
%%   Checkin a Cassandra Connection and return whether the
%%   checkin was successful (it fails if the process is dead
%%   when checkin is attempted), and how many connections
%%   are available after the checkin.
%%
%%   Connections have a fixed probability of failure on checkin.
%%   The decay probability is a number of chances of dying per
%%   1 Billion checkin attempts. If the Connection is killed, it
%%   will be replaced by the supervisor automatically spawning
%%   a new worker and placing it at the end of the queue.
%% @end
checkin_immediate(Config, BS_Module, Node, Connection_Id, Pending_Queue, true) ->
    Connection_Queue = elysium_config:session_queue_name (Config),
    Max_Connections  = elysium_config:session_max_count  (Config),
    case is_process_alive(Connection_Id) of
        false -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_dead),
                 fail_checkin(BS_Module, Connection_Queue, Max_Connections, {Node, Connection_Id}, Config);
        true  -> succ_checkin(BS_Module, Connection_Queue, Max_Connections, {Node, Connection_Id}, Config,
                              Pending_Queue, true)
    end;
checkin_immediate(Config, BS_Module, Node, Connection_Id, Pending_Queue, false) ->
    Connection_Queue = elysium_config:session_queue_name (Config),
    Max_Connections  = elysium_config:session_max_count  (Config),
    case is_process_alive(Connection_Id) of
        false -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, session_dead),
                 fail_checkin(BS_Module, Connection_Queue, Max_Connections, {Node, Connection_Id}, Config);
        true  -> case decay_causes_death(Config, Connection_Id) of
                     false -> succ_checkin(BS_Module, Connection_Queue, Max_Connections,
                                           {Node, Connection_Id}, Config, Pending_Queue, false);
                     true  -> _ = decay_connection(Config, BS_Module, Connection_Id),
                              fail_checkin(BS_Module, Connection_Queue, Max_Connections,
                                           {Node, Connection_Id}, Config)
                  end
    end.

%% Connection_Data is passed to allow tracing
fail_checkin(BS_Module, Connection_Queue, Max_Connections, {_Node, Connection_Id}, Config) ->
    _ = elysium_buffering_audit:audit_data_delete(Config, BS_Module, Connection_Id),
    Available = BS_Module:idle_connection_count(Connection_Queue),
    {false, report_available_resources(Connection_Queue, Available, Max_Connections)}.

succ_checkin(BS_Module, Connection_Queue, Max_Connections, {Node, Connection_Id} = Connection_Data,
             Config, Pending_Queue, Is_New_Connection) ->
    case BS_Module:pending_request_count(Pending_Queue) =:= 0 of
        false -> checkin_pending(Config, BS_Module, Node, Connection_Id, Pending_Queue, Is_New_Connection);
        true  ->
            Available = BS_Module:checkin_connection(Connection_Queue, Connection_Data),
            _ = elysium_buffering_audit:audit_data_checkin(Config, BS_Module, Connection_Id),
            {true, report_available_resources(Connection_Queue, Available, Max_Connections)}
    end.

%% Checkin connection diverted to use for a pending request.
checkin_pending(Config, BS_Module, Node, Sid, Pending_Queue, Is_New_Connection) ->
    case BS_Module:checkout_pending_request(Pending_Queue) of

        %% There are no pending requests, return the session...
        none_available -> checkin_immediate(Config, BS_Module, Node, Sid, Pending_Queue, Is_New_Connection);

        %% Got a pending request, let's run it...
        {{Waiting_Pid, Sid_Reply_Ref}, When_Originally_Queued} ->

            Reply_Timeout = elysium_config:request_reply_timeout(Config),
            case timer:now_diff(os:timestamp(), When_Originally_Queued) of

                %% Too much time has passed, skip this request and try another...
                Expired when Expired > Reply_Timeout * 1000 ->
                    _ = elysium_buffering_audit:audit_count(Config, BS_Module, pending_timeouts),
                    checkin_pending(Config, BS_Module, Node, Sid, Pending_Queue, Is_New_Connection);

                %% There's still time to reply, run the request if the session is still alive.
                _Remaining_Time ->
                    case is_process_alive(Waiting_Pid) of
                        false -> _ = elysium_buffering_audit:audit_count(Config, BS_Module, pending_dead),
                                 checkin_pending(Config, BS_Module, Node, Sid, Pending_Queue, Is_New_Connection);
                        true  -> Waiting_Pid ! {sid, Sid_Reply_Ref, Node, Sid, Pending_Queue, Is_New_Connection},
                                 _ = elysium_buffering_audit:audit_data_pending(Config, BS_Module, Sid),
                                 delay_checkin(Config, BS_Module)
                    end
            end;

        %% Something specific to a BS_Module.
        Error ->
            case BS_Module:checkout_pending_error(Config, BS_Module, Pending_Queue, Error) of
                retry -> checkin_pending(Config, BS_Module, Node, Sid, Pending_Queue, Is_New_Connection);
                Error -> Error
            end
    end.

delay_checkin(Config, BS_Module) ->
    Connection_Queue = elysium_config:session_queue_name  (Config),
    Max_Connections  = elysium_config:session_max_count   (Config),
    Available        = BS_Module:idle_connection_count(Connection_Queue),
    {pending, report_available_resources(Connection_Queue, Available, Max_Connections)}.

decay_causes_death(Config, _Connection_Id) ->
    case elysium_config:decay_probability(Config) of
        Never_Decays when is_integer(Never_Decays), Never_Decays =:= 0 ->
            false;
        Probability  when is_integer(Probability),  Probability   >  0, Probability =< 1000000000 ->
            R = elysium_random:random_int_up_to(1000000000),
            R =< Probability
    end.

decay_connection(Config, BS_Module, Connection_Id) ->
    Supervisor_Pid = elysium_queue:get_connection_supervisor(),
    _ = case elysium_connection_sup:stop_child(Supervisor_Pid, Connection_Id) of
            {error, not_found} -> dont_replace_child;
            ok -> elysium_connection_sup:start_child(Supervisor_Pid, [Config, restart])
        end,
    _ = elysium_buffering_audit:audit_count   (Config, BS_Module, session_decay),
    elysium_buffering_audit:audit_data_delete (Config, BS_Module, Connection_Id).

status(Config) ->
    {status, {idle_connections(Config),
              pending_requests(Config)}}.

queue_status(Config, Queue_Name) ->
    {_Buffering, BS_Module} = elysium_connection:get_buffer_strategy_module(Config),
    BS_Module:queue_status(Queue_Name).

queue_status_reset(Config, Queue_Name) ->
    {_Buffering, BS_Module} = elysium_connection:get_buffer_strategy_module(Config),
    BS_Module:queue_status_reset(Queue_Name).

report_available_resources(Queue_Name, {missing_ets_buffer, Queue_Name}, Max) ->
    {Queue_Name, {missing_ets_buffer, 0, Max}};
report_available_resources(Queue_Name, Num_Sessions, Max) ->
    {Queue_Name, Num_Sessions, Max}.

%% Use the Connection_Id to run the query if we aren't out of time
handle_pending_request(Config, BS_Module, Elapsed_Time, Reply_Timeout, Node, Connection_Id, Query_Request) ->
    %% Self cannot be executed inside the fun(), it needs to be set in the current context.
    Self = self(),
    Worker_Reply_Ref = make_ref(),
    Worker_Fun = fun() -> exec_pending_request(Worker_Reply_Ref, Self, Node, Connection_Id, Query_Request) end,
    {Worker_Pid, Worker_Monitor_Ref} = spawn_opt(Worker_Fun, [monitor]),   % May want to add other options
    Timeout_Remaining = Reply_Timeout - (Elapsed_Time div 1000),
    try   receive_worker_reply(Config, BS_Module, Worker_Reply_Ref,
                               Timeout_Remaining, Worker_Pid, Worker_Monitor_Ref)
    after erlang:demonitor(Worker_Monitor_Ref, [flush])
    end.


%%%-----------------------------------------------------------------------
%%% Internal support functions
%%%-----------------------------------------------------------------------

idle_connections(Config) ->
    Connection_Queue = elysium_config:session_queue_name             (Config),
    Max_Connections  = elysium_config:session_max_count              (Config),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module (Config),
    Idle_Count       = BS_Module:idle_connection_count        (Connection_Queue),
    {idle_connections, report_available_resources(Connection_Queue, Idle_Count, Max_Connections)}.
    
pending_requests(Config) ->
    Pending_Queue    = elysium_config:requests_queue_name            (Config),
    Reply_Timeout    = elysium_config:request_reply_timeout          (Config),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module (Config),
    Pending_Count    = BS_Module:pending_request_count           (Pending_Queue),
    {pending_requests, report_available_resources(Pending_Queue, Pending_Count, Reply_Timeout)}.

%% Worker_Pid is passed to allow tracing
receive_worker_reply(Config, BS_Module, Worker_Reply_Ref, Timeout_Remaining, Worker_Pid, Worker_Monitor_Ref) ->
    receive
        {wrr, Worker_Reply_Ref, Reply} ->
            Reply;
        {'DOWN', Worker_Monitor_Ref, process, Worker_Pid, Reason} ->
            _ = elysium_buffering_audit:audit_count(Config, BS_Module, worker_errors),
            {worker_reply_error, Reason}
    after Timeout_Remaining ->
            _ = elysium_buffering_audit:audit_count(Config, BS_Module, worker_timeouts),
            {worker_reply_timeout, Timeout_Remaining}
    end.

%% Watch Out! This function swaps from the Config on a checkin request to the
%% Config on the original pending query. If you somehow mix connection queues
%% by passing different Configs, the clusters which queries run on may get
%% mixed up resulting in queries/updates/deletes talking to the wrong clusters.
exec_pending_request(Reply_Ref, Reply_Pid, Node, Sid,
                     {bare_fun, Config, Query_Fun, Args, Consistency}) ->
    try   Reply = Query_Fun(Sid, Args, Consistency),
          Reply_Pid ! {wrr, Reply_Ref, Reply}
    catch A:B -> lager:error("Query execution caught ~p:~p for ~p ~p ~9999p~n",
                             [A,B, Reply_Pid, Args, erlang:get_stacktrace()])
    after _ = checkin_connection(Config, Node, Sid, false)
    end;
exec_pending_request(Reply_Ref, Reply_Pid, Node, Sid,
                     {mod_fun,  Config, Mod,  Fun, Args, Consistency}) ->
    try   Reply = Mod:Fun(Sid, Args, Consistency),
          Reply_Pid ! {wrr, Reply_Ref, Reply}
    catch A:B -> lager:error("Query execution caught ~p:~p for ~p ~p ~9999p~n",
                             [A,B, Reply_Pid, Args, erlang:get_stacktrace()])
    after _ = checkin_connection(Config, Node, Sid, false)
    end.
