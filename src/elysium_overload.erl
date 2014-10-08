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
%%% @since 0.1.4
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_overload).
-author('jay@duomark.com').

-behaviour(gen_fsm).

%% External API
-export([
         start_link/0,
         status/0,
         checkin_connection/2,
         buffer_request/1
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

-spec checkin_connection(config_type(), pid())
                        -> {boolean(), {session_queue_name(), Idle_Count, Max_Count}}
                               when Idle_Count :: max_sessions() | ets_buffer:buffer_error(),
                                    Max_Count  :: max_sessions() | ets_buffer:buffer_error().
%% @doc
%%   Checkin a seestar_session, IF there are no pending requests.
%%   One pending request will use the session immediately.
%% @end
checkin_connection(Config, Session_Id)
  when is_pid(Session_Id) ->
    case is_process_alive(Session_Id) of
        false -> elysium_queue:checkin(Config, Session_Id);
        true  -> gen_fsm:sync_send_event(?MODULE, {checkin, Config, Session_Id})
    end.

-spec buffer_request(query_request()) -> any().
buffer_request(Query_Request) ->
    gen_fsm:sync_send_event(?SERVER, {buffer_request, Query_Request}).


%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------

-spec init({}) -> {ok, 'EMPTY', #eo_state{}}.
%% @private
%% @doc Create an empty queue and wait for pending requests.
init({}) ->
    State = #eo_state{requests=queue:new()},
    {ok, ?empty, State}.

%% @private
%% @doc Used only for dynamic code loading.
code_change(_Old_Vsn, State_Name, #eo_state{} = State, _Extra) ->
    {ok, State_Name, State}.

%% @private
%% @doc Report termination.
terminate(Reason, _State_Name, #eo_state{requests=Queue}) ->
    error_logger:error_msg("~p terminated with a queue of ~p requests: ~p~n",
                           [?MODULE, queue:len(Queue), Reason]),
    ok.


%%%-----------------------------------------------------------------------
%%% FSM states
%%%       'EMPTY'        : no pending requests
%%%       'BUFFERING'    : requests are queueing
%%%       'OVERLOADED'   : a spike of new connections has been created
%%%-----------------------------------------------------------------------

%%% Synchronous calls

-spec ?empty({buffer_request, query_request()}, {pid(), reference()}, State)
            -> {next_state, ?buffering, State};

            ({checkin, config_type(), pid()}, {pid(), reference()}, State)
            -> {reply, {boolean(), {session_queue_name(), Idle_Count, Max_Count}}, ?empty, State}
                                      when State :: #eo_state{},
                        Idle_Count :: max_sessions() | ets_buffer:buffer_error(),
                        Max_Count  :: max_sessions() | ets_buffer:buffer_error().
            
%% @private
%% @doc Empty queues stay empty until a queue_request message arrives.
?empty({checkin, Config, Sid}, _From, #eo_state{} = State) ->
    Reply = elysium_queue:checkin(Config, Sid),
    {reply, Reply, ?empty, #eo_state{} = State};
?empty({buffer_request, Query_Request}, _From, #eo_state{requests=Queue} = State) ->
    New_State = State#eo_state{requests=queue:in(Query_Request, Queue)},
    {next_state, ?buffering, New_State};
?empty(Any, _From, #eo_state{} = State) ->
    {reply, {unknown_request, Any}, ?empty, #eo_state{} = State}.


-spec ?buffering({buffer_request, query_request()}, {pid(), reference()}, State)
              -> {next_state, ?buffering, State}
                     when State :: #eo_state{}.
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
?buffering({buffer_request, Query_Request}, _From, #eo_state{requests=Queue} = State) ->
    New_State = State#eo_state{requests=queue:in(Query_Request, Queue)},
    {next_state, ?buffering, New_State};
?buffering(Any, _From, #eo_state{} = State) ->
    {reply, {unknown_request, Any}, ?buffering, State}.


-spec ?overloaded({buffer_request, query_request()}, {pid(), reference()}, State)
              -> {next_state, ?overloaded, State}
                     when State :: #eo_state{}.
%% @private
%% @doc Report overload status.
?overloaded({buffer_request, Query_Request}, _From, #eo_state{requests=Queue} = State) ->
    New_State = State#eo_state{requests=queue:in(Query_Request, Queue)},
    {next_state, ?overloaded, New_State};
?overloaded(Any, _From, #eo_state{} = State) ->
    {reply, {unknown_request, Any}, ?overloaded, State}.


%%% Asynchronous calls

-spec ?empty(any(), State) -> {next_state, ?empty, State} when State :: #eo_state{}.
%% @private
%% @doc No asynch calls to 'EMPTY' supported.
?empty(_Any, #eo_state{} = State) ->
    {next_state, ?empty, State}.

-spec ?buffering(any(), State) -> {next_state, ?buffering, State} when State :: #eo_state{}.
%% @private
%% @doc No asynch calls to 'BUFFERING' supported.
?buffering(_Any, #eo_state{} = State) ->
    {next_state, ?buffering, State}.

-spec ?overloaded(activate, State) -> {next_state, ?overloaded, State} when State :: #eo_state{}.
%% @private
%% @doc No asynch calls to 'OVERLOADED' supported.
?overloaded(_Any, #eo_state{} = State) ->
    {next_state, ?overloaded, State}.


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

-spec handle_info(any(), State_Name, State)
                 -> {next_state, State_Name, State}
                        when State_Name :: fsm_state_fun_name(),
                             State      :: #eo_state{}.
%% @private
%% @doc Unused function.
handle_info (_Any, State_Name, #eo_state{} = State) ->
    {next_state, State_Name, State}.
