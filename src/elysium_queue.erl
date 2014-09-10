%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_fsm is a gen_fsm that owns a FIFO queue of Cassandra
%%%   sessions. This FSM is started before any of the Cassandra
%%%   connections so that the ets_buffer FIFO queue can be owned by
%%%   a process which is less volatile than the individual connections.
%%%   A containing application can crash a connection without disrupting
%%%   other connections, nor disrupting the smooth flow of connection
%%%   checkin / checkout.
%%%
%%%   To avoid serializing requests through this gen_fsm, all queues
%%%   are assumed to be FIFO, dedicated ets_buffers. Queue Name is
%%%   provided by the caller to avoid an ets lookup from the application
%%%   environment. Normally, the caller will use configured_name/0
%%%   to get the Queue Name initially, which is then stored locally
%%%   to the client caller and used on subsequent requests.
%%%
%%%   There currently may only be one elysium_queue per application
%%%   because it is a registered name and there is only one worker
%%%   configuration.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_queue).
-author('jay@duomark.com').

-behaviour(gen_fsm).

%% External API
-export([
         configured_name/0,
         start_link/1,
         idle_connections/1,
         checkout/1,
         checkin/2
        ]).

%% FSM states
-export(['INACTIVE'/2, 'ACTIVE'/2]).
-export(['ACTIVE'/3, 'INACTIVE'/3]).

%% FSM callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-define(SERVER, ?MODULE).
-define(MAX_CHECKOUT_RETRY, 20).

-type ets_buffer_name() :: atom().
-record(ef_state, {buffer_name :: ets_buffer_name()}).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec configured_name() -> undefined | {ok, ets_buffer_name()}.
%% @doc
%%   Fetch the queue name configured as cassandra_worker_queue in the
%%   elysium section of the application config.
%% @end
configured_name() ->
    application:get_env(elysium, cassandra_worker_queue).

-spec start_link(ets_buffer_name()) -> {ok, pid()}.
%% @doc
%%   Create an ets_buffer dedicated FIFO queue using the passed in queue
%%   name.
%% @end
start_link(Queue_Name)
  when is_atom(Queue_Name) ->
    gen_fsm:start_link(?MODULE, {Queue_Name, fifo, dedicated}, []).
    
-spec idle_connections(ets_buffer_name()) -> non_neg_integer().
%% @doc
%%   Report the number of queued connections, since active connections
%%   are removed from the available queue.
%% @end
idle_connections(Queue_Name) ->
    Buffer_Count = ets_buffer:num_entries_dedicated(Queue_Name),
    report_available_sessions(Queue_Name, Buffer_Count).

%% activate  (Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, activate).
%% deactivate(Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, deactivate).
    
-spec checkout(ets_buffer_name()) -> pid() | none_available.
%% @doc
%%   Allocate a seestar_session to the caller by popping an entry
%%   from the front of the connection queue. This function either
%%   returns a live pid(), or none_available to indicate that all
%%   connections to Cassandra are currently checked out.
%%
%%   The queue name must be provided so that we can avoid serializing
%%   all concurrent calls through the underlying gen_fsm.
%% @end
checkout(Queue_Name)
  when is_atom(Queue_Name) ->
    fetch_pid_from_queue(Queue_Name, 1).

%% Internal loop function to retry getting from the queue.
fetch_pid_from_queue(_Queue_Name, ?MAX_CHECKOUT_RETRY) -> none_available;
fetch_pid_from_queue( Queue_Name, Count) ->
    case ets_buffer:read_dedicated(Queue_Name) of

        %% Give up if there are no connections available...
        [] -> none_available;

        %% Race condition with checkin, try again...
        %% (Internally, ets_buffer calls erlang:yield() when this happens)
        {missing_ets_data, Queue_Name, _} ->
            fetch_pid_from_queue(Queue_Name, Count+1);

        %% Return only a live pid, otherwise get the next one.
        [Session_Id] when is_pid(Session_Id) ->
            case is_process_alive(Session_Id) of
                %% NOTE: we toss only MAX_CHECKOUT_RETRY dead pids
                false -> fetch_pid_from_queue(Queue_Name, Count+1);
                true  -> Session_Id
            end;

        %% Somehow the connection buffer died, or something even worse!
        Error -> Error
    end.

-spec checkin(ets_buffer_name(), pid()) -> {boolean(), pos_integer()}.
%% @doc
%%   Checkin a seestar_session by putting it at the end of the
%%   available connection queue. Returns whether the checkin was
%%   successful (it fails if the process is dead when checkin is
%%   attempted), and how many connections are available after the
%%   checkin.
%%
%%   Currently sessions stay active until they fail or are
%%   deliberately ended. In future, this function will include a
%%   small decay factor so that sessions will eventually get
%%   recycled.
%% @end
checkin(Queue_Name, Session_Id)
  when is_atom(Queue_Name), is_pid(Session_Id) ->
    case is_process_alive(Session_Id) of
        false ->
            Available = ets_buffer:num_entries_dedicated(Queue_Name),
            {false, report_available_sessions(Queue_Name, Available)};
        true ->
            Available = ets_buffer:write_dedicated(Queue_Name, Session_Id),
            {true,  report_available_sessions(Queue_Name, Available)}
    end.

report_available_sessions( Queue_Name, {missing_ets_buffer, Queue_Name}) -> 0;
report_available_sessions(_Queue_Name, Num_Sessions) -> Num_Sessions.

     
%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------

%% @private
%% @doc
%%   Create the connection queue and initialize the internal state.
%% @end        
init({Queue_Name, fifo, dedicated}) ->
    ets_buffer:create_dedicated(Queue_Name, fifo),
    State = #ef_state{buffer_name = Queue_Name},
    {ok, 'INACTIVE', State}.

%% @private
%% @doc
%%   Used only for dynamic code loading.
%% @end        
code_change(_Old_Vsn, State_Name, State, _Extra) ->
    {ok, State_Name, State}.

%% @private
%% @doc
%%   Delete the connection queue, presuming that all connections have
%%   already been stopped.
%% @end        
terminate(Reason, _State_Name, #ef_state{buffer_name=Queue_Name}) ->
    error_logger:error_msg("~p for buffer ~p terminated with reason ~p~n",
                           [?MODULE, Queue_Name, Reason]),
    _ = ets_buffer:delete_dedicated(Queue_Name),
    ok.


%%%-----------------------------------------------------------------------
%%% FSM states (currently FSM state is ignored)
%%%       'ACTIVE'   : allocate worker pids
%%%       'INACTIVE' : stop allocating worker pids
%%%-----------------------------------------------------------------------

-spec 'ACTIVE'(any(), {pid(), reference()}, State)
              -> {next_state, 'ACTIVE', State} when State :: #ef_state{}.
%% Synchronous calls are not supported.
%% @private
%% @doc
%%    Stay in the 'ACTIVE' state.
%% @end
'ACTIVE'  (_Any, _From, State) -> {next_state, 'ACTIVE',   State}.

-spec 'INACTIVE'(any(), {pid(), reference()}, State)
              -> {next_state, 'INACTIVE', State} when State :: #ef_state{}.
%% @private
%% @doc
%%    Stay in the 'INACTIVE' state.
%% @end
'INACTIVE'(_Any, _From, State) -> {next_state, 'INACTIVE', State}.

-spec 'INACTIVE'(activate, State) -> {next_state, 'ACTIVE',   State}
%%                (any(),    State) -> {next_state, 'INACTIVE', State}
                                         when State :: #ef_state{}.
%% @private
%% @doc
%%    Activate if requested, from the 'INACTIVE' state. Otherwise
%%    remain in the 'INACTIVE' state.
%% @end
'INACTIVE'(activate, State) -> {next_state, 'ACTIVE',   State};
'INACTIVE'(_Other,   State) -> {next_state, 'INACTIVE', State}.

-spec 'ACTIVE'(activate, State) -> {next_state, 'ACTIVE',   State}
%%              (any(),    State) -> {next_state, 'INACTIVE', State}
                                       when State :: #ef_state{}.
%% @private
%% @doc
%%    Deactivate if requested, from the 'ACTIVE' state. Otherwise
%%    remain in the 'ACTIVE' state.
%% @end
'ACTIVE'(deactivate, State) -> {next_state, 'INACTIVE', State};
'ACTIVE'(_Other,     State) -> {next_state, 'ACTIVE',   State}.


%%%-----------------------------------------------------------------------
%%% Unused callbacks
%%%-----------------------------------------------------------------------

-spec handle_info(any(), State_Name, State)
                 -> {next_state, State_Name, State}
                        when State_Name :: atom(), State :: #ef_state{}.
%% @private
%% @doc Unused function.
handle_info (_Any, State_Name, State) ->
    {next_state, State_Name, State}.

-spec handle_event(any(), State_Name, State)
                 -> {next_state, State_Name, State}
                        when State_Name :: atom(), State :: #ef_state{}.
%% @private
%% @doc Unused function.
handle_event(_Any, State_Name, State) ->
    {next_state, State_Name, State}.

-spec handle_sync_event(any(), {pid(), reference()}, State_Name, State)
                 -> {reply, ok, State_Name, State}
                        when State_Name :: atom(), State :: #ef_state{}.
%% @private
%% @doc Unused function
handle_sync_event(_Any, _From, State_Name, State) ->
    {reply, ok, State_Name, State}.
