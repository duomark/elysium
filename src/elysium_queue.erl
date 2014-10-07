%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_fsm is a gen_fsm that owns a FIFO queue of available
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
%%%   may be established. The list of hosts is kept in a queue so that
%%%   every attempt to connect or reconnect is a round-robin request against
%%%   a different host, skipping any that cannot successfully accept a new
%%%   connection request. Coupled with stochastic decay of existing connections
%%%   the active sessions will automatically adapt to the spontaneous
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
         register_connection_supervisor/1,

         activate/0,
         deactivate/0,
         enable/0,

         idle_connections/1,
         checkout/1,
         checkin/2
        ]).

%% FSM states
-define(active,        'ACTIVE').
-define(disabled,      'DISABLED').
-define(inactive,      'INACTIVE').
-define(wait_register, 'WAIT_REGISTER').

-type fsm_state_fun_name() :: ?active | ?disabled | ?inactive | ?wait_register.
-type fsm_state_name()     ::  active  | disabled  | inactive  | wait_register.

-export([?active/2, ?disabled/2, ?inactive/2, ?wait_register/2]).
-export([?active/3, ?disabled/3, ?inactive/3, ?wait_register/3]).

%% FSM callbacks
-export([
         init/1,
         handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4
        ]).

-define(SERVER, ?MODULE).

-include("elysium_types.hrl").

-record(ef_state, {
          config          :: config_type(),
          available_hosts :: lb_queue_name(),
          live_sessions   :: session_queue_name(),
          connection_sup  :: pid()
         }).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
%% @doc Create an ets_buffer dedicated FIFO queue using the configured queue name.
start_link(Config) ->
    true = elysium_config:is_valid_config(Config),
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, {Config}, []).

-spec register_connection_supervisor(pid()) -> ok.
%% @doc Register the connection supervisor so that it can be manually controlled.
register_connection_supervisor(Connection_Sup)
  when is_pid(Connection_Sup) ->
    gen_fsm:sync_send_all_state_event(?MODULE, {register_connection_supervisor, Connection_Sup}).

-spec activate() -> Max_Allowed::max_sessions().
%% @doc Change to the active state, creating new Cassandra sessions.
activate() ->
    gen_fsm:sync_send_event(?MODULE, activate).

-spec deactivate() -> {Num_Terminated::max_sessions(), Max_Allowed::max_sessions()}.
%% @doc Change to the inactive state, deleting Cassandra sessions.
deactivate() ->
    gen_fsm:sync_send_event(?MODULE, deactivate).

-spec enable() -> ok.
%% @doc Change from disabled to the inactive state, allowing new Cassandra sessions to be created.
enable() ->
    gen_fsm:sync_send_event(?MODULE, enable).
    
-spec idle_connections(config_type()) -> {session_queue_name(),
                                          {active, Idle_Count, Max_Count}
                                          | {inactive, Idle_Count, Max_Count}
                                          | {disabled, Idle_Count, Max_Count}
                                          | {missing_buffer, Idle_Count, Max_Count}}
                                             when Idle_Count :: max_sessions(),
                                                  Max_Count  :: max_sessions().
%% @doc Idle connections are those in the queue, not checked out.
idle_connections(Config) ->
    Queue_Name   = elysium_config:session_queue_name (Config),
    Max_Sessions = elysium_config:session_max_count  (Config),
    Buffer_Count = ets_buffer:num_entries_dedicated(Queue_Name),
    report_available_sessions(Queue_Name, Buffer_Count, Max_Sessions).

report_available_sessions(Queue_Name, {missing_ets_buffer, Queue_Name}, Max) ->
    {Queue_Name, {missing_buffer, 0, Max}};
report_available_sessions(Queue_Name, Num_Sessions, Max) ->
    {Queue_Name, {get_current_state(), Num_Sessions, Max}}.

get_current_state() ->
    gen_fsm:sync_send_all_state_event(?MODULE, current_state).
    
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

-spec checkin(config_type(), pid()) -> {boolean(), {session_queue_name(), {fsm_state_name(), Idle_Count, Max_Count}}}
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
checkin(Config, Session_Id)
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


%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------

-spec init({config_type()}) -> {ok, 'WAIT_REGISTER', #ef_state{}}.
%% @private
%% @doc
%%   Create the connection queue and initialize the internal state.
%% @end        
init({Config}) ->
    %% Setup a load balancing FIFO Queue for all the Cassandra nodes to contact.
    Lb_Queue_Name = elysium_config:load_balancer_queue(Config),
    ets_buffer:create_dedicated(Lb_Queue_Name, fifo),
    _ = [ets_buffer:write_dedicated(Lb_Queue_Name, Node)
         || {_Ip, _Port} = Node <- elysium_config:round_robin_hosts(Config),
            is_list(_Ip), is_integer(_Port), _Port > 0],

    %% Create a FIFO Queue for the live sessions that are connected to Cassandra.
    Session_Queue_Name = elysium_config:session_queue_name(Config),
    ets_buffer:create_dedicated(Session_Queue_Name, fifo),

    %% Setup the internal state to be able to reference the queues.
    State = #ef_state{config = Config, available_hosts = Lb_Queue_Name,
                      live_sessions = Session_Queue_Name},
    {ok, ?wait_register, State}.

%% @private
%% @doc
%%   Used only for dynamic code loading.
%% @end        
code_change(_Old_Vsn, State_Name, #ef_state{} = State, _Extra) ->
    {ok, State_Name, State}.

%% @private
%% @doc
%%   Delete the connection queue, stopping all idle connections have
%%   already been stopped.
%% @end        
terminate(Reason, _State_Name,
          #ef_state{available_hosts=Lb_Queue_Name, live_sessions=Session_Queue_Name}) ->
    error_logger:error_msg("~p for load balancer ~p and session queue ~p terminated with reason ~p~n",
                           [?MODULE, Lb_Queue_Name, Session_Queue_Name, Reason]),

    %% Stop all idle sessions...
    _ = case ets_buffer:read_all_dedicated(Session_Queue_Name) of
            Sessions when is_list(Sessions) ->
                [elysium_connection:stop(Sid) || Sid <- Sessions];
            _Error -> done
        end,

    %% Can't do anything about checked out sessions
    %% (maybe we should keep a checked out ets table?)
    _ = ets_buffer:delete_dedicated(Session_Queue_Name),
    _ = ets_buffer:delete_dedicated(Lb_Queue_Name),
    ok.


%%%-----------------------------------------------------------------------
%%% FSM states
%%%       'ACTIVE'        : allocate session pids
%%%       'DISABLED'      : application configured elysium off
%%%       'INACTIVE'      : terminate session pids
%%%       'WAIT_REGISTER' : waiting for elysium_connection_sup to start
%%%-----------------------------------------------------------------------

%%% Synchronous calls

-spec ?active(deactivate, {pid(), reference()}, State)
              -> {reply, ok, ?inactive, State} when State :: #ef_state{}.
%% @private
%% @doc Move to 'INACTIVE' if requested, otherwise stay in 'ACTIVE' state.
?active(deactivate, _From, #ef_state{config=Config} = State) ->
    Max_Sessions = elysium_config:session_max_count(Config),
    Kids = supervisor:which_children(elysium_connection_sup),
    _ = [supervisor:terminate_child(elysium_connection_sup, Pid) || {undefined, Pid, _, _} <- Kids],
    {reply, {length(Kids), Max_Sessions}, ?inactive, #ef_state{} = State};
?active  (_Any, _From, #ef_state{} = State) ->
    {reply, ok, ?active, State}.

-spec ?disabled(enable, {pid(), reference()}, State)
              -> {reply, inactive, ?inactive, State} when State :: #ef_state{}.
%% @private
%% @doc Move to 'INACTIVE' if enabled, otherwise stay in 'DISABLED' state.
?disabled(enable, _From, #ef_state{} = State) -> {reply, inactive, ?inactive, State};
?disabled(_Any,   _From, #ef_state{} = State) -> {reply, disabled, ?disabled, State}.

-spec ?inactive(activate, {pid(), reference()}, State)
              -> {reply, max_sessions(), ?active, State} when State :: #ef_state{}.
%% @private
%% @doc Move to 'ACTIVE' if requested, otherwise stay in 'INACTIVE' state.
?inactive(activate, _From, #ef_state{connection_sup=Conn_Sup, config=Config} = State) ->
    Max_Sessions = elysium_config:session_max_count(Config),
    _ = [elysium_connection_sup:start_child(Conn_Sup, [Config])
         || _N <- lists:seq(1, Max_Sessions)],
    {reply, Max_Sessions, ?active, State};
?inactive(_Any, _From, #ef_state{} = State) ->
    {reply, ok, ?inactive, State}.

-spec ?wait_register(deactivate, {pid(), reference()}, State)
                    -> {reply, ok, ?inactive, State} when State :: #ef_state{}.
%% @private
%% @doc Stay in the 'WAIT_REGISTER' state.
?wait_register(deactivate, _From, #ef_state{} = State) -> {reply, ok, ?inactive,      State};
?wait_register(_Any,       _From, #ef_state{} = State) -> {reply, ok, ?wait_register, State}.


%%% Asynchronous calls

-spec ?active(activate, State) -> {next_state, ?active, State} when State :: #ef_state{}.
%% @private
%% @doc Deactivate if requested, from the 'ACTIVE' state.
?active(deactivate, #ef_state{} = State) ->
    Kids = supervisor:which_children(elysium_sup),
    _ = [supervisor:terminate_child(elysium_sup, Id) || {Id, _, _, _} <- Kids],
    {next_state, ?inactive, State};
?active(_Other, #ef_state{} = State) ->
    {next_state, ?active,   State}.

-spec ?disabled(enable, State) -> {next_state, ?inactive, State} when State :: #ef_state{}.
%% @private
%% @doc Move to 'INACTIVE' if enabled, otherwise stay in 'DISABLED' state.
?disabled(enable, #ef_state{} = State) -> {next_state, ?inactive, State};
?disabled(_Any,   #ef_state{} = State) -> {next_state, ?disabled, State}.

-spec ?inactive(activate, State) -> {next_state, ?active, State} when State :: #ef_state{}.
%% @private
%% @doc Activate if requested, from the 'INACTIVE' state.
?inactive(activate, #ef_state{connection_sup=Conn_Sup, config=Config} = State) ->
    Max_Sessions = elysium_config:session_max_count(Config),
    _ = [elysium_connection_sup:start_child(Conn_Sup, [Config])
         || _N <- lists:seq(1, Max_Sessions)],
    {next_state, ?active, State};
?inactive(_Other, #ef_state{} = State) ->
    {next_state, ?inactive, State}.

-spec ?wait_register(deactivate, State) -> {next_state, ?inactive, State} when State :: #ef_state{}.
%% @private
%% @doc Deactivate if requested, from the 'WAIT_REGISTER' state.
?wait_register(deactivate, #ef_state{} = State) -> {next_state, ?inactive,      State};
?wait_register(_Other,     #ef_state{} = State) -> {next_state, ?wait_register, State}.


%%%-----------------------------------------------------------------------
%%% Event callbacks
%%%-----------------------------------------------------------------------

-spec handle_sync_event(current_state | {register_connection_supervisor, pid()},
                        {pid(), reference()}, State_Name, State)
                 -> {reply, fsm_state_name() | ok | {error, tuple()}, State_Name, State}
                        when State_Name :: fsm_state_fun_name(),
                             State      :: #ef_state{}.
%% @private
%% @doc Register the connection supervisor; report the current state of the FSM.
handle_sync_event({register_connection_supervisor, Connection_Sup} = Event, _From,
                  ?wait_register = State_Name, #ef_state{connection_sup=undefined} = State)
  when is_pid(Connection_Sup) ->
    New_State = State#ef_state{connection_sup=Connection_Sup},
    case application:get_env(elysium) of
        undefined     -> {reply, ok, ?disabled, New_State};
        {ok, enabled} -> {reply, ok, ?inactive, New_State};
        _Disabled     -> {reply, ok, ?disabled, New_State}
    end;
handle_sync_event({register_connection_supervisor, _Connection_Sup} = Event, _From, State_Name, #ef_state{} = State) ->
    error_logger:error_msg("Unexpected event ~p for state name ~p in state ~p~n", [Event, State_Name, State]),
    {reply, {error, {wrong_state, State_Name}}, State_Name, State};

handle_sync_event(current_state, _From, ?active,        #ef_state{} = State) -> {reply, active,        ?active,        State};
handle_sync_event(current_state, _From, ?disabled,      #ef_state{} = State) -> {reply, disabled,      ?disabled,      State};
handle_sync_event(current_state, _From, ?inactive,      #ef_state{} = State) -> {reply, active,        ?active,        State};
handle_sync_event(current_state, _From, ?wait_register, #ef_state{} = State) -> {reply, wait_register, ?wait_register, State}.


%%%-----------------------------------------------------------------------
%%% Unused callbacks
%%%-----------------------------------------------------------------------

-spec handle_event(any(), State_Name, State)
                 -> {next_state, State_Name, State}
                        when State_Name :: fsm_state_fun_name(),
                             State      :: #ef_state{}.
%% @private
%% @doc Unused function.
handle_event(_Any, State_Name, #ef_state{} = State) ->
    {next_state, State_Name, State}.

-spec handle_info(any(), State_Name, State)
                 -> {next_state, State_Name, State}
                        when State_Name :: fsm_state_fun_name(),
                             State      :: #ef_state{}.
%% @private
%% @doc Unused function.
handle_info (_Any, State_Name, #ef_state{} = State) ->
    {next_state, State_Name, State}.
