%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_bs_serial is a buffering strategy which enforces serial
%%%   initiation of Cassandra session queries, although the duration of
%%%   a query will vary so they are not guaranteed to execute in serial
%%%   order. This approach allows spikes in the traffic which exceed the
%%%   number of availabel elysium sessions. The buffer is maintained as
%%%   a FIFO ets_buffer, so it has overhead when unloading expired pending
%%%   requests when compared to a LIFO buffer for the same task. All
%%%   requests attempt to fetch an idle session before being added to
%%%   the end of the pending queue.
%%%
%%% @since 0.1.5
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_bs_serial).
-author('jay@duomark.com').

%% External API
-export([
         checkin_connection/4,
         checkout_connection/1,
         pend_request/2,
         status/1
        ]).

%% FSM states
-type fun_request()   :: fun((pid(), [any()], seestar:consistency()) -> [any()]).
-type query_request() :: {bare_fun, config_type(), fun_request(), [any()], seestar:consistency()}
                       | {mod_fun, config_type(), module(), atom(), [any()], seestar:consistency()}.

-define(SERVER, ?MODULE).

-include("elysium_types.hrl").


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-type resource_counts() :: {idle_connections | pending_requests,
                            {atom(), non_neg_integer(), non_neg_integer()}}.
-spec status(config_type()) -> {status, [resource_counts()]}.
%% @doc Get the current queue size of the pending queue.
status(Config) ->
    {status, [idle_connections(Config),
              pending_requests(Config)]}.

-spec checkout_connection(config_type()) -> {{Ip::string(), Port::pos_integer()}, pid()} | none_available.
%% @doc
%%   Allocate a seestar_session to the caller by popping an entry
%%   from the front of the connection queue. This function either
%%   returns a live pid(), or none_available to indicate that all
%%   connections to Cassandra are currently checked out.
%%
%%   If there are internal delays on the ets_buffer FIFO queue,
%%   this function will retry. If the session handed back is no
%%   longer live, it is tossed and a new session is fetched. In
%%   both cases, up to Config_Module:checkout_max_retry attempts
%%   are tried before returning none_available. If the queue is
%%   actually empty, no retries are performed.
%%
%%   The configuration parameter is not validated because this
%%   function should be a hotspot and we don't want it to slow
%%   down or become a concurrency bottleneck.
%% @end
checkout_connection(Config) ->
    Queue_Name  = elysium_config:session_queue_name (Config),
    Max_Retries = elysium_config:checkout_max_retry (Config),
    fetch_pid_from_queue(Queue_Name, Max_Retries, -1).

-spec checkin_connection(config_type(), {Ip::string(), Port::pos_integer()},
                         Session_Id::pid(), Is_New_Connection::boolean())
                        -> {boolean() | pending, {session_queue_name(), Idle_Count, Max_Count}}
                               when Idle_Count :: max_sessions() | ets_buffer:buffer_error(),
                                    Max_Count  :: max_sessions() | ets_buffer:buffer_error().
%% @doc
%%   Checkin a seestar_session, IF there are no pending requests.
%%   A checkin will continue looping on the pending queue with
%%   the chance for decay on each attempt. If it decays, any
%%   newly spawned replacement is expected to check the pending
%%   queue for outstanding requests. Brand new connections are
%%   not checked for decay before first use.
%%
%%   This function can loop forever if there are pending requests,
%%   so it performs an asynchronous send_event.
%% @end
checkin_connection(Config, {_Ip, _Port} = Node, Session_Id, Is_New_Connection)
  when is_pid(Session_Id) ->
    Pending_Queue = elysium_config:requests_queue_name(Config),
    case is_process_alive(Session_Id) andalso ets_buffer:num_entries_dedicated(Pending_Queue) > 0 of
        false -> checkin_immediate (Config, Node, Session_Id, Pending_Queue, Is_New_Connection);
        true  -> checkin_pending   (Config, Node, Session_Id, Pending_Queue, Is_New_Connection)
    end.

-spec pend_request(config_type(), query_request()) -> any() | pend_request_error().
%%   Block the caller while the request is serially queued. When
%%   a session is avialable to run this pending request, the
%%   blocking recieve loop will unblock and a spawned process
%%   will execute the request, so that the caller can still
%%   timeout if the request takes too long.
%% @end
pend_request(Config, Query_Request) ->
    Sid_Reply_Ref = make_ref(),
    Start_Time    = os:timestamp(),
    Pending_Queue = elysium_config:requests_queue_name   (Config),
    Reply_Timeout = elysium_config:request_reply_timeout (Config),
    wait_for_session(Config, Pending_Queue, Sid_Reply_Ref, Start_Time, Query_Request, Reply_Timeout).


%%%-----------------------------------------------------------------------
%%% Internal support functions
%%%-----------------------------------------------------------------------

idle_connections(Config) ->
    Queue_Name   = elysium_config:session_queue_name (Config),
    Max_Sessions = elysium_config:session_max_count  (Config),
    Buffer_Count = ets_buffer:num_entries_dedicated (Queue_Name),
    {idle_connections, report_available_resources(Queue_Name, Buffer_Count, Max_Sessions)}.
    
pending_requests(Config) ->
    Pending_Queue = elysium_config:requests_queue_name   (Config),
    Reply_Timeout = elysium_config:request_reply_timeout (Config),
    Pending_Count = ets_buffer:num_entries_dedicated (Pending_Queue),
    {pending_requests, report_available_resources(Pending_Queue, Pending_Count, Reply_Timeout)}.

report_available_resources(Queue_Name, {missing_ets_buffer, Queue_Name}, Max) ->
    {Queue_Name, {missing_ets_buffer, 0, Max}};
report_available_resources(Queue_Name, Num_Sessions, Max) ->
    {Queue_Name, Num_Sessions, Max}.

%% Internal loop function to retry getting from the queue.
fetch_pid_from_queue(_Queue_Name, Max_Retries, Times_Tried) when Times_Tried >= Max_Retries -> none_available;
fetch_pid_from_queue( Queue_Name, Max_Retries, Times_Tried) ->
    case ets_buffer:read_dedicated(Queue_Name) of

        %% Give up if there are no connections available...
        [] -> none_available;

        %% Race condition with checkin, try again...
        %% (Internally, ets_buffer calls erlang:yield() when this happens)
        {missing_ets_data, Queue_Name, _} ->
            fetch_pid_from_queue(Queue_Name, Max_Retries, Times_Tried+1);

        %% Return only a live pid, otherwise get the next one.
        [{_Node, Session_Id} = Session_Data] when is_pid(Session_Id) ->
            case is_process_alive(Session_Id) of
                %% NOTE: we toss only MAX_CHECKOUT_RETRY dead pids
                false -> fetch_pid_from_queue(Queue_Name, Max_Retries, Times_Tried+1);
                true  -> Session_Data
            end;

        %% Somehow the connection buffer died, or something even worse!
        Error ->
            lager:error("Connection buffer error: ~9999p~n", [Error]),
            Error
    end.

wait_for_session(Config, Pending_Queue, Sid_Reply_Ref, Start_Time, Query_Request, Reply_Timeout) ->
    Queue_Name = elysium_config:session_queue_name(Config),
    case fetch_pid_from_queue(Queue_Name, 1, 0) of

        %% A free connection showed up since we first checked...
        {Node, Session_Id} ->
            handle_pending_request(0, Reply_Timeout, Node, Session_Id, Query_Request);

        %% None are still available, queue the request and wait for one to free up.
        _None_Available ->
            _Pending_Count = ets_buffer:write_dedicated(Pending_Queue, {{self(), Sid_Reply_Ref}, Start_Time}),
            receive

                %% A live elysium session channel is now available to make the request...
                {sid, Sid_Reply_Ref, Node, Session_Id, Pending_Queue, Is_New_Connection} ->

                    Elapsed_Time = timer:now_diff(os:timestamp(), Start_Time),
                    case {Elapsed_Time >= Reply_Timeout * 1000, is_process_alive(Session_Id)} of

                        %% Alas, we timed out waiting...
                        {true,  true}  -> _ = checkin_connection(Config, Node, Session_Id, Is_New_Connection),
                                          {wait_for_session_timeout, Reply_Timeout};
                        {true,  false} -> {wait_for_session_timeout, Reply_Timeout};

                        %% Dead session, loop waiting for another (hopefully live) connection to free up...
                        {false, false} -> New_Timeout = Reply_Timeout - (Elapsed_Time div 1000),
                                          wait_for_session(Config, Pending_Queue, Sid_Reply_Ref,
                                                           Start_Time, Query_Request, New_Timeout);

                        %% Get some results while we still have time!
                        {false, true}  -> handle_pending_request(Elapsed_Time, Reply_Timeout,
                                                                 Node, Session_Id, Query_Request)
                    end

                %% Any other messages are intended for the blocked caller, leave them in the message queue.

            after Reply_Timeout -> {wait_for_session_timeout, Reply_Timeout}
            end
    end.

%% Use the Session_Id to run the query if we aren't out of time
handle_pending_request(Elapsed_Time, Reply_Timeout, Node, Session_Id, Query_Request) ->
    %% Self cannot be executed inside the fun(), it needs to be set in the current context.
    Self = self(),
    Worker_Reply_Ref = make_ref(),
    %% Avoiding export of exec_pending_request/5
    Worker_Fun = fun() -> exec_pending_request(Worker_Reply_Ref, Self, Node, Session_Id, Query_Request) end,
    {Worker_Pid, Worker_Monitor_Ref} = spawn_opt(Worker_Fun, [monitor]),   % May want to add other options
    Timeout_Remaining = Reply_Timeout - (Elapsed_Time div 1000),
    try   receive_worker_reply(Worker_Reply_Ref, Timeout_Remaining, Worker_Pid, Worker_Monitor_Ref)
    after erlang:demonitor(Worker_Monitor_Ref, [flush])
    end.

%% Worker_Pid is passed to allow tracing
receive_worker_reply(Worker_Reply_Ref, Timeout_Remaining, Worker_Pid, Worker_Monitor_Ref) ->
    receive
        {wrr, Worker_Reply_Ref, Reply}                            -> Reply;
        {'DOWN', Worker_Monitor_Ref, process, Worker_Pid, Reason} -> {worker_reply_error, Reason}
    after Timeout_Remaining                          -> {worker_reply_timeout, Timeout_Remaining}
    end.

-spec checkin_immediate(config_type(), {Ip::string(), Port::pos_integer()},
                        Session_Id::pid(), Pending_Queue::requests_queue_name(), Is_New_Connection::boolean())
                       -> {boolean(), {session_queue_name(), Idle_Count, Max_Count}}
                              when Idle_Count :: max_sessions() | ets_buffer:buffer_error(),
                                   Max_Count  :: max_sessions() | ets_buffer:buffer_error().
%% @doc
%%   Checkin a seestar_session by putting it at the end of the
%%   available connection queue. Returns whether the checkin was
%%   successful (it fails if the process is dead when checkin is
%%   attempted), and how many connections are available after the
%%   checkin.
%%
%%   Sessions have a fixed probability of failure on checkin.
%%   The decay probability is a number of chances of dying per
%%   1 Million checkin attempts. If the session is killed, it
%%   will be replaced by the supervisor automatically spawning
%%   a new worker and placing it at the end of the queue.
%%
%%   The configuration parameter is not validated because this
%%   function should be a hotspot and we don't want it to slow
%%   down or become a concurrency bottleneck.
%% @end
checkin_immediate(Config, Node, Session_Id, Pending_Queue, true) ->
    Queue_Name   = elysium_config:session_queue_name (Config),
    Max_Sessions = elysium_config:session_max_count  (Config),
    case is_process_alive(Session_Id) of
        false -> fail_checkin(Queue_Name, Max_Sessions, {Node, Session_Id});
        true  -> succ_checkin(Queue_Name, Max_Sessions, {Node, Session_Id}, Config, Pending_Queue, true)
    end;
checkin_immediate(Config, Node, Session_Id, Pending_Queue, false) ->
    Queue_Name   = elysium_config:session_queue_name (Config),
    Max_Sessions = elysium_config:session_max_count  (Config),
    case is_process_alive(Session_Id) of
        false -> fail_checkin(Queue_Name, Max_Sessions, {Node, Session_Id});
        true  -> case decay_causes_death(Config, Session_Id) of
                     false -> succ_checkin(Queue_Name, Max_Sessions, {Node, Session_Id},
                                           Config, Pending_Queue, false);
                     true  -> _ = decay_session(Config, Session_Id),
                              fail_checkin(Queue_Name, Max_Sessions, {Node, Session_Id})
                  end
    end.

%% Session_Data is passed to allow tracing
fail_checkin(Queue_Name, Max_Sessions, _Session_Data) ->
    Available = ets_buffer:num_entries_dedicated(Queue_Name),
    {false, report_available_resources(Queue_Name, Available, Max_Sessions)}.

succ_checkin(Queue_Name, Max_Sessions, {Node, Session_Id} = Session_Data,
             Config, Pending_Queue, Is_New_Connection) ->
    case ets_buffer:num_entries_dedicated(Pending_Queue) > 0 of
        true  -> checkin_pending(Config, Node, Session_Id, Pending_Queue, Is_New_Connection);
        false -> Available = ets_buffer:write_dedicated(Queue_Name, Session_Data),
                 {true, report_available_resources(Queue_Name, Available, Max_Sessions)}
    end.

delay_checkin(Config) ->
    Queue_Name    = elysium_config:session_queue_name  (Config),
    Max_Sessions  = elysium_config:session_max_count   (Config),
    Available     = ets_buffer:num_entries_dedicated (Queue_Name),
    {pending, report_available_resources(Queue_Name, Available, Max_Sessions)}.

decay_causes_death(Config, _Session_Id) ->
    case elysium_config:decay_probability(Config) of
        Never_Decays when is_integer(Never_Decays), Never_Decays =:= 0 ->
            false;
        Probability  when is_integer(Probability),  Probability   >  0, Probability =< 1000000 ->
            R = elysium_random:random_int_up_to(1000000),
            R =< Probability
    end.

decay_session(Config, Session_Id) ->
    Supervisor_Pid = elysium_queue:get_connection_supervisor(),
    case elysium_connection_sup:stop_child(Supervisor_Pid, Session_Id) of
        {error, not_found} -> dont_replace_child;
        ok -> elysium_connection_sup:start_child(Supervisor_Pid, [Config])
    end.

checkin_pending(Config, Node, Sid, Pending_Queue, Is_New_Connection) ->
    case ets_buffer:read_dedicated(Pending_Queue) of

        %% There are no pending requests, return the session...
        [] -> checkin_immediate(Config, Node, Sid, Pending_Queue, Is_New_Connection);

        %% Race condition with pend_request, try again...
        %% (Internally, ets_buffer calls erlang:yield() when this happens)
        %% (It is presumed that repeated calling will eventually yield something even [])
        {missing_ets_data, Pending_Queue, _} ->
            checkin_pending(Config, Node, Sid, Pending_Queue, Is_New_Connection);

        %% Got a pending request, let's run it...
        [{{Waiting_Pid, Sid_Reply_Ref}, When_Originally_Queued}] ->

            Reply_Timeout = elysium_config:request_reply_timeout(Config),
            case timer:now_diff(os:timestamp(), When_Originally_Queued) of

                %% Too much time has passed, skip this request and try another...
                Expired when Expired > Reply_Timeout * 1000 ->
                    checkin_pending(Config, Node, Sid, Pending_Queue, Is_New_Connection);

                %% There's still time to reply, run the request if the session is still alive.
                _Remaining_Time ->
                    case is_process_alive(Waiting_Pid) of
                        false -> checkin_pending(Config, Node, Sid, Pending_Queue, Is_New_Connection);
                        true  -> Waiting_Pid ! {sid, Sid_Reply_Ref, Node, Sid, Pending_Queue, Is_New_Connection},
                                 delay_checkin(Config)
                    end
            end;

        %% Somehow the pending buffer died, or something even worse!
        Error ->
            lager:error("Pending requests buffer error: ~9999p~n", [Error]),
            Error
    end.

%% Watch Out! This function swaps from the Config on a checkin request to the
%% Config on the original pending query. If you somehow mix connection queues
%% by passing different Configs, the clusters which queries run on may get
%% mixed up resulting in queries/updates/deletes talking to the wrong clusters.
exec_pending_request(Reply_Ref, Reply_Pid, Node, Sid, {bare_fun, Config, Query_Fun, Args, Consistency}) ->
    try   Reply = Query_Fun(Sid, Args, Consistency),
          Reply_Pid ! {wrr, Reply_Ref, Reply}
    catch A:B -> error_logger:error_msg("Query execution caught ~p:~p for ~p ~p ~9999p~n",
                                        [A,B, Reply_Pid, Args, erlang:get_stacktrace()])
    after _ = checkin_connection(Config, Node, Sid, false)
    end;
exec_pending_request(Reply_Ref, Reply_Pid, Node, Sid, {mod_fun,  Config, Mod,  Fun, Args, Consistency}) ->
    try   Reply = Mod:Fun(Sid, Args, Consistency),
          Reply_Pid ! {wrr, Reply_Ref, Reply}
    catch A:B -> error_logger:error_msg("Query execution caught ~p:~p for ~p ~p ~9999p~n",
                                        [A,B, Reply_Pid, Args, erlang:get_stacktrace()])
    after _ = checkin_connection(Config, Node, Sid, false)
    end.
