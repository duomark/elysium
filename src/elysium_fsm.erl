%%%-----------------------------------------------------------------------
%%% elysium_fsm is a gen_fsm that manages a FIFO queue of workers.
%%%-----------------------------------------------------------------------
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

-define(DEFAULT_BUFFER, elysium_workers).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
-type buffer_name() :: atom().

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(?DEFAULT_BUFFER, fifo, dedicated).
    
-spec start_link(buffer_name(), fifo | lifo, dedicated | shared) -> {ok, pid()}.
start_link(Buffer_Name, Buffer_Type, Is_Dedicated)
  when is_atom(Buffer_Name),
       (Buffer_Type =:= fifo orelse Buffer_Type =:= lifo),
       (Is_Dedicated =:= dedicated orelse Is_Dedicated =:= shared) ->
    Args = {Buffer_Name, Buffer_Type, Is_Dedicated},
    gen_fsm:start_link(?MODULE, Args, []).

%% activate  (Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, activate).
%% deactivate(Fsm_Ref) -> gen_fsm:send_event(Fsm_Ref, deactivate).
    

%%%-----------------------------------------------------------------------
%%% init, code_change and terminate
%%%-----------------------------------------------------------------------
        
init({Buffer_Name, Buffer_Type, Is_Dedicated}) ->
    case Is_Dedicated of
        dedicated -> ets_buffer:create_dedicated (Buffer_Name, Buffer_Type);
        shared    -> ets_buffer:create           (Buffer_Name, Buffer_Type)
    end,
    State = #ef_state{
               buffer_name  = Buffer_Name,
               buffer_type  = Buffer_Type,
               is_dedicated = (Is_Dedicated =:= dedicated)
              },
    {ok, 'INACTIVE', State}.

code_change(_Old_Vsn, State_Name, State, _Extra) -> {ok, State_Name, State}.

terminate(Reason, _State_Name, #ef_state{} = State) ->
    #ef_state{buffer_name=Buffer_Name, is_dedicated=Is_Dedicated} = State,
    Error_Args = [?MODULE, Buffer_Name, Reason],
    error_logger:error_msg("~p for buffer ~p terminated with reason ~p", Error_Args),
    _ = case Is_Dedicated of
            true  -> ets_buffer:delete_dedicated (Buffer_Name);
            false -> ets_buffer:delete           (Buffer_Name)
        end,
    ok.


%%%-----------------------------------------------------------------------
%%% FSM states
%%%       'ACTIVE'   : allocate worker pids
%%%       'INACTIVE' : stop allocating worker pids
%%%-----------------------------------------------------------------------

%% Synchronous calls are not supported.
'ACTIVE'  (_Any, _From, State) -> {next_state, 'ACTIVE',   State}.
'INACTIVE'(_Any, _From, State) -> {next_state, 'INACTIVE', State}.

%% Asynchronous calls just change the internal state, possibly emitting stats.
'INACTIVE'(activate, State) -> {next_state, 'ACTIVE',   State};
'INACTIVE'(_Other,   State) -> {next_state, 'INACTIVE', State}.

'ACTIVE'(deactivate, State) -> {next_state, 'INACTIVE', State};
'ACTIVE'(_Other,     State) -> {next_state, 'ACTIVE',   State}.


%%%-----------------------------------------------------------------------
%%% Unused callbacks
%%%-----------------------------------------------------------------------

handle_info (_Any, State_Name, State) -> {next_state, State_Name, State}.
handle_event(_Any, State_Name, State) -> {next_state, State_Name, State}.
handle_sync_event(_Any, _From, State_Name, State) -> {reply, ok, State_Name, State}.
