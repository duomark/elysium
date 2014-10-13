%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_overload offers optional buffering of connection requests.
%%%   This approach allows spikes in the traffic which exceed the number
%%%   of available elysium sessions. The buffer is maintained as an in
%%%   memory erlang Queue of requests by unloading them from the FSM
%%%   message mailbox as the requests arrive and sending no reply to the
%%%   requestor. Any subsequent checkins will match against the buffered
%%%   queue prior to being checked in.
%%%
%%%   If the buffered queue grows too big, a one-time state change causes
%%%   a fresh set of sessions to be spawned to empty the buffer. These
%%%   new sessions are unsupervised and will stick around only until they
%%%   decay. This provides a way to absorb a spike in traffic, however, it
%%%   could lead to overload on the Cassandra cluster. Be careful when
%%%   using this feature as it masks the backpressure signal when all
%%%   sessions are occupied.
%%%
%%% @since 0.1.5
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_overload).
-author('jay@duomark.com').

-behaviour(gen_fsm).

%% External API
-export([
         start_link/0,
         status/0,

         checkout_connection/1,
         checkin_connection/2,
         idle_connections/1,
         pending_requests/1,
         buffer_request/2
        ]).

%% FSM states
-define(empty,      'EMPTY').
-define(buffering,  'BUFFERING').
-define(overloaded, 'OVERLOADED').

-type fsm_state_fun_name() :: ?empty | ?buffering | ?overloaded.
-type fsm_state_name()     ::  empty  | buffering  | overloaded.
-type query_request()      :: {bare_fun, config_type(), fun((pid(), [any()], seestar:consistency()) -> [any()]),
                                                           [any()], seestar:consistency()}
                            | {mod_fun, config_type(), module(), atom(), [any()], seestar:consistency()}.

-export([?empty/2, ?buffering/2, ?overloaded/2]).
-export([?empty/3, ?buffering/3, ?overloaded/3]).

%% FSM callbacks
-export([
         init/1,
         handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4
        ]).

-define(SERVER, ?MODULE).

-include("elysium_types.hrl").

-record(eo_state, {
          requests :: queue()
         }).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
%% @doc Create an FSM to track pending requests in a queue.
start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, {}, []).

-spec status() -> {fsm_state_name(), non_neg_integer()}.
%% @doc Get the current status and queue size of the FSM.
status() ->
    gen_fsm:sync_send_all_state_event(?SERVER, status).
    
-spec idle_connections(config_type()) -> {session_queue_name(), Idle_Count, Max_Count}
                                          | {session_queue_name(), {missing_buffer, Idle_Count, Max_Count}}
                                             when Idle_Count :: max_sessions(),
                                                  Max_Count  :: max_sessions().
%% @doc Idle connections are those in the queue, not checked out.
idle_connections(Config) ->
    Queue_Name   = elysium_config:session_queue_name (Config),
    Max_Sessions = elysium_config:session_max_count  (Config),
    Buffer_Count = ets_buffer:num_entries_dedicated (Queue_Name),
    report_available_sessions(Queue_Name, Buffer_Count, Max_Sessions).
    
-spec pending_requests(config_type()) -> {requests_queue_name(), Pending_Count, Reply_Timeout}
                                          | {requests_queue_name(), {missing_buffer, 0, Reply_Timeout}}
                                             when Pending_Count :: non_neg_integer(),
                                                  Reply_Timeout :: non_neg_integer().
%% @doc Pending requests are queries waiting for a free session.
pending_requests(Config) ->
    Queue_Name    = elysium_config:requests_queue_name   (Config),
    Reply_Timeout = elysium_config:request_reply_timeout (Config),
    Pending_Count = ets_buffer:num_entries_dedicated (Queue_Name),
    report_available_sessions(Queue_Name, Pending_Count, Reply_Timeout).

-spec checkout_connection(config_type()) -> pid() | none_available.
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
%%   are tried before returning none_available.
%%
%%   If the queue is actually empty, no retries are performed.
%%   It is left to the application in this case to decide when
%%   and how often to retry.
%%
%%   The configuration parameter is not validated because this
%%   function should be a hotspot and we don't want it to slow
%%   down or become a concurrency bottleneck.
%% @end
checkout_connection(Config) ->
    Queue_Name  = elysium_config:session_queue_name (Config),
    Max_Retries = elysium_config:checkout_max_retry (Config),
    fetch_pid_from_queue(Queue_Name, Max_Retries, -1).

-spec checkin_connection(config_type(), pid())
                        -> {boolean(), {session_queue_name(), Idle_Count, Max_Count}}
                               when Idle_Count :: max_sessions() | ets_buffer:buffer_error(),
                                    Max_Count  :: max_sessions() | ets_buffer:buffer_error().
%% @doc
%%   Checkin a seestar_session, IF there are no pending requests.
%%   One pending request will use the session immediately before
%%   the session is checked in with the possibility of decay.
%%
%%   Because this function is likely called from a try / after
%%   construct and the return value is ignored, it is safe to
%%   execute this asynchronously. The real reason to do it in
%%   an asynchronous call is that the original user of a session
%%   might be penalized with latency waiting on a second user
%%   when in an overload situation. To avoid this possibility,
%%   we run the checkin asynchronous, possbily spawning a new
%%   process to execute the query so as not to block the FSM
%%   from handling other checkin and buffer requests.
%% @end
checkin_connection(Config, Session_Id)
  when is_pid(Session_Id) ->
    case is_process_alive(Session_Id) of
        false -> checkin_immediate(Config, Session_Id);
        true  -> gen_fsm:send_event(?MODULE, {checkin, Config, Session_Id})
    end.

-spec buffer_request(config_type(), query_request()) -> any() | ets_buffer:buffer_error().
%%   Synchronously queue the current request so that the caller can wait
%%   for a reply. This function makes a call into the gen_fsm but it
%%   doesn't reply right away. Later, when a seestar_session is available
%%   it will execute the query and reply back to the original requestor.
%% @end
buffer_request(Config, Query_Request) ->
    Reply_Ref     = make_ref(),
    Timestamp     = os:timestamp(),
    Pending_Queue = elysium_config:requests_queue_name   (Config),
    Reply_Timeout = elysium_config:request_reply_timeout (Config),
    case ets_buffer:write_dedicated(Pending_Queue, {{self(), Reply_Ref}, Timestamp}) of
        Available when is_integer(Available), Available > 0 ->
            receive {Reply_Ref, Session_Id} -> exec_pending_request(Session_Id, Query_Request)
            after   Reply_Timeout -> {timeout, Reply_Timeout}
            end
    end.


%%%-----------------------------------------------------------------------
%%% Internal support functions
%%%-----------------------------------------------------------------------

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
        [Session_Id] when is_pid(Session_Id) ->
            case is_process_alive(Session_Id) of
                %% NOTE: we toss only MAX_CHECKOUT_RETRY dead pids
                false -> fetch_pid_from_queue(Queue_Name, Max_Retries, Times_Tried+1);
                true  -> Session_Id
            end;

        %% Somehow the connection buffer died, or something even worse!
        Error -> Error
    end.

-spec checkin_immediate(config_type(), pid())
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
checkin_immediate(Config, Session_Id)
  when is_pid(Session_Id) ->
    Queue_Name   = elysium_config:session_queue_name (Config),
    Max_Sessions = elysium_config:session_max_count  (Config),
    case is_process_alive(Session_Id) of
        false -> fail_checkin(Queue_Name, Max_Sessions);
        true  -> case decay_causes_death(Config, Session_Id) of
                     false -> succ_checkin(Queue_Name, Max_Sessions, Session_Id);
                     true  -> exit(Session_Id, kill),
                              fail_checkin(Queue_Name, Max_Sessions)
                  end
    end.

fail_checkin(Queue_Name, Max_Sessions) ->
    Available = ets_buffer:num_entries_dedicated(Queue_Name),
    {false, report_available_sessions(Queue_Name, Available, Max_Sessions)}.

succ_checkin(Queue_Name, Max_Sessions, Session_Id) ->
    Available = ets_buffer:write_dedicated(Queue_Name, Session_Id),
    {true, report_available_sessions(Queue_Name, Available, Max_Sessions)}.

decay_causes_death(Config, _Session_Id) ->
    case elysium_config:decay_probability(Config) of
        Never_Decays when is_integer(Never_Decays), Never_Decays =:= 0 ->
            false;
        Probability  when is_integer(Probability),  Probability   >  0, Probability =< 1000000 ->
            R = elysium_random:random_int_up_to(1000000),
            R =< Probability
    end.

report_available_sessions(Queue_Name, {missing_ets_buffer, Queue_Name}, Max) ->
    {Queue_Name, {missing_buffer, 0, Max}};
report_available_sessions(Queue_Name, Num_Sessions, Max) ->
    {Queue_Name, Num_Sessions, Max}.


%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------

-spec init({}) -> {ok, 'EMPTY', #eo_state{}}.
%% @private
%% @doc Create an empty queue and wait for pending requests.
init({}) ->
    State = #eo_state{},
    {ok, ?empty, State}.

%% @private
%% @doc Used only for dynamic code loading.
code_change(_Old_Vsn, State_Name, #eo_state{} = State, _Extra) ->
    {ok, State_Name, State}.

%% @private
%% @doc Report termination.
terminate(Reason, _State_Name, #eo_state{}) ->
    error_logger:error_msg("~p terminated: ~p~n", [?MODULE, Reason]),
    ok.


%%%-----------------------------------------------------------------------
%%% FSM states
%%%       'EMPTY'        : no pending requests
%%%       'BUFFERING'    : requests are queueing
%%%       'OVERLOADED'   : a spike of new connections has been created
%%%-----------------------------------------------------------------------

%%% Synchronous calls

-spec ?empty({buffer_request, query_request()}, {pid(), reference()}, State)
            -> {reply, {unknown_request, any()}, ?empty, State} when State :: #eo_state{}.
%% @private
%% @doc Empty queues stay empty until a queue_request message arrives.
?empty(Any, _From, #eo_state{} = State) ->
    {reply, {unknown_request, Any}, ?empty, #eo_state{} = State}.


-spec ?buffering(Request, {pid(), reference()}, State)
              -> {reply, {unknown_request, Request}, ?buffering, State}
                     when Request :: any(),
                          State   :: #eo_state{}.
%% @private
%% @doc
%%   When buffering, new requests are added to the internal queue, but
%%   checkin requests are immediately handed to the next queued request.
%%   Checkin requests are only used once, and then a an unbuffered checkin
%%   is called to give the session a chance to decay.
%%
%%   If the queue of buffered tasks grows to the configuration overload_spike
%%   size, the FSM transitions to the 'OVERLOADED' state. 
%% @end
?buffering(Any, _From, #eo_state{} = State) ->
    {reply, {unknown_request, Any}, ?buffering, State}.


-spec ?overloaded(Request, {pid(), reference()}, State)
                 -> {reply, {unknown_request, Request}, ?overloaded, State}
                        when Request :: any(),
                             State   :: #eo_state{}.
%% @private
%% @doc Report overload status.
?overloaded(Any, _From, #eo_state{} = State) ->
    {reply, {unknown_request, Any}, ?overloaded, State}.


%%% Asynchronous calls

-spec ?empty({checkin, config_type(), pid()}, State) -> {next_state, ?empty, State} when State :: #eo_state{}.
%% @private
%% @doc No asynch calls to 'EMPTY' supported.
?empty({checkin, Config, Sid}, #eo_state{} = State) ->
    Pending_Queue = elysium_config:requests_queue_name(Config),
    case ets_buffer:num_entries_dedicated(Pending_Queue) of
          0 -> checkin_empty_pending(Config, Sid, State);
        _N1 -> case ets_buffer:read_dedicated(Pending_Queue) of
                  [] -> checkin_empty_pending(Config, Sid, State);
                  %% Handle missing_ets_data here
                  [{{From_Pid, Ref}, When_Originally_Queued}] ->
                       Reply_Timeout = elysium_config:request_reply_timeout (Config),
                      case timer:now_diff(os:timestamp(), When_Originally_Queued) of
                          Expired when Expired > Reply_Timeout*1000 -> ?empty({checkin, Config, Sid}, State);
                          _Remaining_Time ->
                              From_Pid ! {Ref, Sid},
                              %% _ = spawn_monitor(fun() -> exec_pending_request(Sid, Query_Request) end),
                              case ets_buffer:num_entries_dedicated(Pending_Queue) of
                                    0 -> {next_state, ?empty,     State};
                                  _N2 -> {next_state, ?buffering, State}
                              end
                      end
              end
    end;
%% ?empty({checkin, Config, Sid}, #eo_state{} = State) ->
%%     _ = checkin_immediate(Config, Sid),
%%     {next_state, ?empty, #eo_state{} = State};
?empty(_Any, #eo_state{} = State) ->
    {next_state, ?empty, State}.

-spec ?buffering(any(), State) -> {next_state, ?buffering, State} when State :: #eo_state{}.
%% @private
%% @doc Checkin of session id gets reused once asynchronously to handle a buffered request.
?buffering({checkin, Config, Sid}, #eo_state{} = State) ->
    Pending_Queue = elysium_config:requests_queue_name(Config),
    Data = ets_buffer:read_dedicated(Pending_Queue),
    error_logger:error_msg("Buffering Read dedicated: ~p~n", [Data]),
    case Data of
        [] -> checkin_empty_pending(Config, Sid, State);
        %% Handle missing_ets_data here
        [{{From_Pid, Ref}, When_Originally_Queued}] ->
            Reply_Timeout = elysium_config:request_reply_timeout (Config),
            case timer:now_diff(os:timestamp(), When_Originally_Queued) of
                Expired when Expired > Reply_Timeout * 1000 ->
                    error_logger:error_msg("Expired time ~p, checking in immediately", [Expired]),
                    ?buffering({checkin, Config, Sid}, State);
                _Remaining_Time ->
                    From_Pid ! {Ref, Sid},
                    %% _ = spawn_monitor(fun() -> exec_pending_request(Sid, Query_Request) end),
                    case ets_buffer:num_entries_dedicated(Pending_Queue) of
                         0 -> {next_state, ?empty,     State};
                        _N -> {next_state, ?buffering, State}
                    end
            end
    end;
?buffering(_Any, #eo_state{} = State) ->
    {next_state, ?buffering, State}.

-spec ?overloaded(activate, State) -> {next_state, ?overloaded, State} when State :: #eo_state{}.
%% @private
%% @doc No asynch calls to 'OVERLOADED' supported.
?overloaded(_Any, #eo_state{} = State) ->
    {next_state, ?overloaded, State}.

checkin_empty_pending(Config, Sid, State) ->
    _ = checkin_immediate(Config, Sid),
    {next_state, ?empty, State}.

%% Watch Out! This function swaps from the Config on a checkin request to the
%% Config on the original pending query. If you somehow mix connection queues
%% by passing different Configs, the clusters which queries run on may get
%% mixed up resulting in queries/updates/deletes talking to the wrong clusters.
exec_pending_request(Sid, {bare_fun, Config, Query_Fun, Args, Consistency}) ->
    try   Query_Fun(Sid, Args, Consistency)
    after _ = checkin_immediate(Config, Sid)
    end;
exec_pending_request(Sid, {mod_fun,  Config, Mod,  Fun, Args, Consistency}) ->
    try   Mod:Fun(Sid, Args, Consistency)
    after _ = checkin_immediate(Config, Sid)
    end.


%%%-----------------------------------------------------------------------
%%% Event callbacks
%%%-----------------------------------------------------------------------

-spec handle_sync_event(status, {pid(), reference()}, State_Name, State)
                 -> {reply, {fsm_state_name(), Queue_Len::non_neg_integer()}, State_Name, State}
                        when State_Name :: fsm_state_fun_name(),
                             State      :: #eo_state{}.
%% @private
%% @doc Report the current state and queue length.
handle_sync_event(status, _From, ?empty,      #eo_state{} = State) ->
    report_queue_length(empty,     ?empty,      State);
handle_sync_event(status, _From, ?buffering,  #eo_state{} = State) ->
    report_queue_length(buffering, ?buffering,  State);
handle_sync_event(status, _From, ?overloaded, #eo_state{} = State) ->
    report_queue_length(overload,  ?overloaded, State).

report_queue_length(Current_Status, Current_State_Name, #eo_state{requests=Queue} = State) ->
    {reply, {Current_Status, queue:len(Queue)}, Current_State_Name, State}.


%%%-----------------------------------------------------------------------
%%% Info callbacks
%%%-----------------------------------------------------------------------

-spec handle_info(any(), State_Name, State)
                 -> {next_state, State_Name, State}
                        when State_Name :: fsm_state_fun_name(),
                             State      :: #eo_state{}.
%% @private
%% @doc Reports any failed pending query results occurring in spawned pending request results.
handle_info ({'DOWN', _Ref, process, _Pid, normal}, State_Name, #eo_state{} = State) ->
    {next_state, State_Name, State};
handle_info ({'DOWN', _Ref, process, _Pid, Reason}, State_Name, #eo_state{} = State) ->
    error_logger:error_msg("Failed query during ~p: ~p~n", [State_Name, Reason]),
    {next_state, State_Name, State};
handle_info (Unhandled_Msg, State_Name, #eo_state{} = State) ->
    error_logger:error_msg("Unknown message ~p received in ~p", [Unhandled_Msg, ?MODULE]),
    {next_state, State_Name, State}.


%%%-----------------------------------------------------------------------
%%% Unused callbacks
%%%-----------------------------------------------------------------------

-spec handle_event(any(), State_Name, State)
                 -> {next_state, State_Name, State}
                        when State_Name :: fsm_state_fun_name(),
                             State      :: #eo_state{}.
%% @private
%% @doc Unused function.
handle_event(_Any, State_Name, #eo_state{} = State) ->
    {next_state, State_Name, State}.
