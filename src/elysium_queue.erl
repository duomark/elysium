%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_fsm is a gen_fsm that handles client queries about the
%%%   state of the session, pending_requests and load balancing
%%%   ets_buffer queues. These queues are owned by elysium_buffer_sup
%%%   but are queried by elysium_queue so that there is no risk of
%%%   a query crashing the owner of an ets table.
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
         get_connection_supervisor/0,

         activate/0,
         deactivate/0,
         enable/0,
         node_change/1
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
          live_sessions   :: connection_queue_name(),
          requests        :: requests_queue_name(),
          connection_sup  :: pid()
         }).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
%% @doc Create an ets_buffer FIFO queue using the configured queue name.
start_link(Config) ->
    true = elysium_config:is_valid_config(Config),
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, {Config}, []).

-spec register_connection_supervisor(pid()) -> ok.
%% @doc Register the connection supervisor so that it can be manually controlled.
register_connection_supervisor(Connection_Sup)
  when is_pid(Connection_Sup) ->
    Register_Cmd = {register_connection_supervisor, Connection_Sup},
    gen_fsm:sync_send_all_state_event(?SERVER, Register_Cmd).

-spec get_connection_supervisor() -> pid().
%% @doc Get the registered connection supervisor.
get_connection_supervisor() ->
    gen_fsm:sync_send_all_state_event(?SERVER, get_connection_supervisor).

-spec activate() -> Max_Allowed::max_connections().
%% @doc Change to the active state, creating new Cassandra sessions.
activate() ->
    gen_fsm:sync_send_event(?SERVER, activate).

-spec deactivate() -> {Num_Terminated::max_connections(), Max_Allowed::max_connections()}.
%% @doc Change to the inactive state, deleting Cassandra sessions.
deactivate() ->
    gen_fsm:sync_send_event(?SERVER, deactivate).

-spec enable() -> ok.
%% @doc Change disabled to inactive state, allowing new session creation.
enable() ->
    gen_fsm:sync_send_event(?SERVER, enable).

-spec node_change([{inet:hostname(), inet:port_number()}]) -> ok.
%% @doc Notifies the queue that the node list changed
node_change(Nodes) ->
    gen_fsm:sync_send_event(?SERVER, {node_change, Nodes}).


%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------

-spec init({config_type()}) -> {ok, 'WAIT_REGISTER', #ef_state{}}.
%% @private
%% @doc
%%   Create the connection queue and initialize the internal state.
%% @end        
init({Config}) ->
    %% Setup the internal state to be able to reference the queues.
    Lb_Queue_Name      = elysium_config:load_balancer_queue (Config),
    Session_Queue_Name = elysium_config:session_queue_name  (Config),
    Pending_Queue_Name = elysium_config:requests_queue_name (Config),
    State = #ef_state{
               config          = Config,
               available_hosts = Lb_Queue_Name,
               live_sessions   = Session_Queue_Name,
               requests        = Pending_Queue_Name
              },
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
?active(deactivate, _From, #ef_state{config=Config, connection_sup=Conn_Sup} = State) ->
    Max_Connections = elysium_config:session_max_count(Config),
    Kids = supervisor:which_children(Conn_Sup),
    _ = [supervisor:terminate_child(Conn_Sup, Pid) || {undefined, Pid, _, _} <- Kids],
    {reply, {length(Kids), Max_Connections}, ?inactive, #ef_state{} = State};
?active  (_Any, _From, #ef_state{} = State) ->
    {reply, ok, ?active, State}.

-spec ?disabled(enable, {pid(), reference()}, State)
              -> {reply, inactive, ?inactive, State} when State :: #ef_state{}.
%% @private
%% @doc Move to 'INACTIVE' if enabled, otherwise stay in 'DISABLED' state.
?disabled(enable, _From, #ef_state{} = State) -> {reply, inactive, ?inactive, State};
?disabled(_Any,   _From, #ef_state{} = State) -> {reply, disabled, ?disabled, State}.

-spec ?inactive(activate, {pid(), reference()}, State)
              -> {reply, max_connections(), ?active, State} when State :: #ef_state{}.
%% @private
%% @doc Move to 'ACTIVE' if requested, otherwise stay in 'INACTIVE' state.
?inactive(activate, _From, #ef_state{config=Config, connection_sup=Conn_Sup} = State) ->
    Lb_Queue_Name   = elysium_config:load_balancer_queue (Config),
    Max_Connections = elysium_config:session_max_count   (Config),
    Num_Nodes       = elysium_lb_queue:num_entries(Lb_Queue_Name),
    _ = [spawn_monitor(elysium_connection_sup, start_child, [Conn_Sup, [Config]])
         || _N <- lists:seq(1, Max_Connections * Num_Nodes)],
    {reply, Max_Connections, ?active, State};
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
?active(deactivate, #ef_state{connection_sup=Conn_Sup} = State) ->
    Kids = supervisor:which_children(Conn_Sup),
    _ = [supervisor:terminate_child(Conn_Sup, Id) || {Id, _, _, _} <- Kids],
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
?inactive(activate, #ef_state{config=Config, connection_sup=Conn_Sup} = State) ->
    Max_Connections = elysium_config:session_max_count   (Config),
    Lb_Queue_Name   = elysium_config:load_balancer_queue (Config),
    Num_Nodes       = ets_buffer:num_entries_dedicated (Lb_Queue_Name),
    _ = [spawn_monitor(elysium_connection_sup, start_child, [Conn_Sup, [Config]])
         || _N <- lists:seq(1, Max_Connections * Num_Nodes)],
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

-type sync_from() :: {pid(), reference()}.
-spec handle_sync_event(current_state, sync_from(), State_Name, State) ->
                               {reply, fsm_state_name(), State_Name, State}
                                   when State_Name :: fsm_state_fun_name(),
                                        State      :: #ef_state{};
                       (get_connection_supervisor, sync_from(), State_Name, State) ->
                               {reply, pid(), State_Name, State}
                                   when State_Name :: fsm_state_fun_name(),
                                        State      :: #ef_state{};
                       ({register_connection_supervisor, pid()}, sync_from(), ?wait_register, State)
                       -> {reply, ok | {error, tuple()}, ?disabled | ?inactive, New_State}
                              when State      :: #ef_state{},
                                   New_State  :: #ef_state{}.
%% @private
%% @doc Register or get the connection supervisor; report the current state of the FSM.
handle_sync_event({register_connection_supervisor, Connection_Sup}, _From,
                  ?wait_register, #ef_state{connection_sup=undefined, config=Config} = State)
  when is_pid(Connection_Sup) ->
    New_State = State#ef_state{connection_sup=Connection_Sup},
    case elysium_config:is_elysium_config_enabled(Config) of
        false -> {reply, ok, ?disabled, New_State};
        true  -> {reply, ok, ?inactive, New_State}
    end;
handle_sync_event({register_connection_supervisor, _Connection_Sup} = Event, _From, State_Name, #ef_state{} = State) ->
    error_logger:error_msg("Unexpected event ~p for state name ~p in state ~p~n", [Event, State_Name, State]),
    {reply, {error, {wrong_state, State_Name}}, State_Name, State};

handle_sync_event(get_connection_supervisor, _From, State_Name, #ef_state{} = State) ->
    {reply, State#ef_state.connection_sup, State_Name, State};

handle_sync_event(current_state, _From, ?active,        #ef_state{} = State) -> {reply, active,        ?active,        State};
handle_sync_event(current_state, _From, ?disabled,      #ef_state{} = State) -> {reply, disabled,      ?disabled,      State};
handle_sync_event(current_state, _From, ?inactive,      #ef_state{} = State) -> {reply, active,        ?inactive,      State};
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
handle_info ({'DOWN', _Ref, process,  Connection_Attempt, Reason}, State_Name, #ef_state{} = State) ->
    Reason =:= normal
        orelse lager:warn("Elysium connection in process ~p could not be established: ~p~n",
                          [Connection_Attempt, Reason]),
    {next_state, State_Name, State};
handle_info (_Any, State_Name, #ef_state{} = State) ->
    {next_state, State_Name, State}.
