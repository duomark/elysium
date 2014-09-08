%%% @doc
%%%   Elysium_fsm is a gen_fsm that owns a FIFO queue of workers.
%%%   This FSM is started before any of the Cassandra connections
%%%   so that the ets_buffer FIFO queue can be owned by a process
%%%   which is less volatile than the individual connections. This
%%%   allows a containing application to crash a connection without
%%%   disrupting other connections, nor disrupting the smooth flow
%%%   of connection checkin / checkout.
%%% @end
-module(elysium_fsm).
-author('jay@duomark.com').

-behaviour(gen_fsm).

%% External API
-export([
         start_link/0, start_link/3
        ]).

%% FSM states
-export(['INACTIVE'/2, 'ACTIVE'/2]).
-export(['ACTIVE'/3, 'INACTIVE'/3]).

%% FSM callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(ef_state, {
          buffer_name          :: atom(),
          buffer_type  = fifo  :: ets_buffer:buffer_type(),
          is_dedicated = true  :: boolean()
         }).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
-type buffer_name() :: atom().

-spec start_link() -> {ok, pid()}.
%% @doc
%%   Create an ets_buffer FIFO queue based on the cassandra_worker_queue
%%   name specified in the application configuration. It defaults to a
%%   dedicated ets table of the same name. This is the recommended
%%   functional call. It requires the including application to specify
%%   an application configuration with all the parameters shown in
%%   the config/sys.config example provided with the source code.
%% @end
start_link() ->
    {ok, Session_Queue_Name} = application:get_env(elysium, cassandra_worker_queue),
    start_link(Session_Queue_Name, fifo, dedicated).
    
-spec start_link(buffer_name(), fifo | lifo, dedicated | shared) -> {ok, pid()}.
%% @doc
%%   Create an ets_buffer FIFO or LIFO queue based on the name passed
%%   in. The queue may be dedicated to its own ets table or shared with
%%   ets_buffers in the application.
%% @end
start_link(Session_Queue_Name, Queue_Type, Is_Dedicated)
  when is_atom(Session_Queue_Name),
       (Queue_Type =:= fifo orelse Queue_Type =:= lifo),
       (Is_Dedicated =:= dedicated orelse Is_Dedicated =:= shared) ->
    Args = {Session_Queue_Name, Queue_Type, Is_Dedicated},
    gen_fsm:start_link(?MODULE, Args, []).

%% activate  (Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, activate).
%% deactivate(Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, deactivate).
    

%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------

%% @private
%% @doc
%%   Create the connection queue and initialize the internal state.
%% @end        
init({Queue_Name, Queue_Type, Is_Dedicated}) ->
    case Is_Dedicated of
        dedicated -> ets_buffer:create_dedicated (Queue_Name, Queue_Type);
        shared    -> ets_buffer:create           (Queue_Name, Queue_Type)
    end,
    State = #ef_state{
               buffer_name  = Queue_Name,
               buffer_type  = Queue_Type,
               is_dedicated = (Is_Dedicated =:= dedicated)
              },
    {ok, 'INACTIVE', State}.

%% @private
%% @doc
%%   Used only for dynamic code loading.
%% @end        
code_change(_Old_Vsn, State_Name, State, _Extra) -> {ok, State_Name, State}.

%% @private
%% @doc
%%   Delete the connection queue, presuming that all connections have
%%   already been stopped.
%% @end        
terminate(Reason, _State_Name, #ef_state{} = State) ->
    #ef_state{buffer_name=Queue_Name, is_dedicated=Is_Dedicated} = State,
    Error_Args = [?MODULE, Queue_Name, Reason],
    error_logger:error_msg("~p for buffer ~p terminated with reason ~p", Error_Args),
    _ = case Is_Dedicated of
            true  -> ets_buffer:delete_dedicated (Queue_Name);
            false -> ets_buffer:delete           (Queue_Name)
        end,
    ok.


%%%-----------------------------------------------------------------------
%%% FSM states
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
                 -> {next_state, State_Name, State} when State_Name :: atom(),
                                                         State :: #ef_state{}.
%% @doc
%%   Unused function.
%% @end
handle_info (_Any, State_Name, State) -> {next_state, State_Name, State}.

-spec handle_event(any(), State_Name, State)
                 -> {next_state, State_Name, State} when State_Name :: atom(),
                                                         State :: #ef_state{}.
%% @doc
%%   Unused function.
%% @end
handle_event(_Any, State_Name, State) -> {next_state, State_Name, State}.

-spec handle_sync_event(any(), {pid(), reference()}, State_Name, State)
                 -> {reply, ok, State_Name, State} when State_Name :: atom(),
                                                        State :: #ef_state{}.
%% @doc
%%   Unused function.
%% @end
handle_sync_event(_Any, _From, State_Name, State) -> {reply, ok, State_Name, State}.
