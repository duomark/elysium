%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_fsm is a gen_fsm that owns a RING buffer of available
%%%   Cassandra hosts and a FIFO queue of pre-allocated Cassandra
%%%   sessions. This FSM is started before any of the Cassandra
%%%   connections so that the ets_buffer queues can be owned by
%%%   a process which is less volatile than the individual connections.
%%%   A containing application can crash a connection without disrupting
%%%   other connections, nor disrupting the smooth flow of connection
%%%   checkin / checkout.
%%%
%%%   To further support fault tolerance and load balancing, the configuration
%%%   data identifies a list of Cassandra hosts to which live sessions
%%%   may be established. The list of hosts is kept in a ring buffer so that
%%%   every attempt to connect or reconnect is a round-robin request against
%%%   a different host, skipping any that cannot successfully accept a new
%%%   connection request. Coupled with stochastic decay of existing connections
%%%   the currently active sessions will automatically adapt to the current
%%%   availability of Cassandra cluster client nodes for transactions.
%%%
%%%   There currently may only be one elysium_queue per application
%%%   because it is a registered name and there is only one worker
%%%   configuration. In future it is hoped that the registered
%%%   ets table name constraint will be removed so that multiple
%%%   independent Cassandra clusters may be used in a single
%%%   application, however, this may prove difficult to do without
%%%   introducing a serial bottleneck or highly contended ets table.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_queue).
-author('jay@duomark.com').

-behaviour(gen_fsm).

%% External API
-export([
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

-include("elysium_types.hrl").

-record(ef_state, {buffer_name :: queue_name()}).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
%% @doc Create an ets_buffer dedicated FIFO queue using the configured queue name.
start_link(Config) ->
    true = elysium_config:is_valid_config(Config),
    gen_fsm:start_link(?MODULE, {Config, fifo, dedicated}, []).
    
-spec idle_connections(config_type()) -> {queue_name(), max_sessions()}.
%% @doc Idle connections are those in the queue, not checked out.
idle_connections(Config) ->
    Queue_Name = elysium_config:session_queue_name(Config),
    Buffer_Count = ets_buffer:num_entries_dedicated(Queue_Name),
    report_available_sessions(Queue_Name, Buffer_Count).


%% activate  (Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, activate).
%% deactivate(Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, deactivate).
    
-spec checkout(config_type()) -> pid() | none_available.
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
checkout(Config) ->
    Queue_Name  = elysium_config:session_queue_name (Config),
    Max_Retries = elysium_config:checkout_max_retry (Config),
    fetch_pid_from_queue(Queue_Name, Max_Retries, -1).

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

-spec checkin(config_type(), pid()) -> {boolean(), {queue_name(), max_sessions()}}.
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
checkin(Config, Session_Id)
  when is_pid(Session_Id) ->
    Queue_Name = elysium_config:session_queue_name(Config),
    case is_process_alive(Session_Id) of
        false ->
            Available = ets_buffer:num_entries_dedicated(Queue_Name),
            {false, report_available_sessions(Queue_Name, Available)};
        true ->
            case decay_causes_death(Config, Session_Id) of
                true  -> exit(Session_Id, kill),
                         Available = ets_buffer:num_entries_dedicated(Queue_Name),
                         {false, report_available_sessions(Queue_Name, Available)};
                false -> Available = ets_buffer:write_dedicated(Queue_Name, Session_Id),
                         {true, report_available_sessions(Queue_Name, Available)}
            end
    end.

report_available_sessions(Queue_Name, {missing_ets_buffer, Queue_Name}) -> {Queue_Name, 0};
report_available_sessions(Queue_Name,                     Num_Sessions) -> {Queue_Name, Num_Sessions}.

decay_causes_death(Config, _Session_Id) ->
    case elysium_config:decay_probability(Config) of
        Never_Decays when is_integer(Never_Decays), Never_Decays =:= 0 -> false;
        Probability  when is_integer(Probability),  Probability   >  0, Probability =< 1000000 ->
            _ = maybe_seed(),
            R = random:uniform(1000000),
            R =< Probability
    end.

maybe_seed() ->
    case get(random_seed) of
        undefined -> random:seed(os:timestamp());
        {X, X, X} -> random:seed(os:timestamp());
        _         -> ok
    end.


%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------

%% @private
%% @doc
%%   Create the connection queue and initialize the internal state.
%% @end        
init({Config, fifo, dedicated}) ->
    Queue_Name = elysium_config:session_queue_name(Config),
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
