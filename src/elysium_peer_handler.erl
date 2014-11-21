-module(elysium_peer_handler).
-author('hernan@inakanetworks.com').

-behaviour(gen_server).

%% API exports.
-export([
         start_link/1,
         get_nodes/0,
         update_nodes/0,
         update_config/1
        ]).

%% gen_server exports.
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-include("elysium_types.hrl").
-include_lib("seestar/src/seestar_messages.hrl").

-record(state, {
          nodes = []         :: [cassandra_node()],
          config = undefined :: undefined | config_type(),
          timer = undefined  :: undefined | timer:tref()
         }).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
%% @doc 
start_link(Config) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec get_nodes() -> [cassandra_node()].
%% @doc Returns the list of nodes, this is updated every minute
get_nodes() -> gen_server:call(?MODULE, {get_nodes}).

-spec update_nodes() -> ok.
%% @doc Updates the node list, it is automatically called, but can be manually invoked
update_nodes() -> gen_server:cast(?MODULE, {update_nodes}).

-spec update_config(config_type()) -> [binary()].
%% @doc Updates the configuration used when re requesting the nodes
update_config(Config) -> gen_server:call(?MODULE, {update_config, Config}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

-spec init(config_type()) -> {ok, #state{}}.

init(Config) -> update_nodes(), {ok, #state{config = Config}}.
terminate(_Reason, _St) -> ok.

handle_call({get_nodes}, _From, #state{nodes = Nodes} = St) ->
    {reply, Nodes, St};
handle_call({update_config, New_Config}, _From, St) ->
    {reply, ok, St#state{config = New_Config}}.

handle_info({update_nodes, New_Nodes}, #state{nodes = Old_Nodes} = St) ->
    case New_Nodes -- Old_Nodes of
        [] -> {noreply, St};
        _  -> New_State = handle_node_change(New_Nodes, St),
              {noreply, New_State}
    end.

handle_cast({update_nodes}, #state{config = Config, timer = Timer} = St) ->
    {ok, _Status} = case timer:cancel(Timer) of
                        {error, _} -> {ok, no_timer};
                        Success    -> Success
                    end,
    ok = update_nodes(Config, self()),
    {ok, New_Timer} = timer:apply_after(timeout(Config), ?MODULE, update_nodes, []),
    {noreply, St#state{timer = New_Timer}}.

%% Unused callbacks

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%% -------------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------------

update_nodes(Config, Pid) ->
    _Pid = spawn(fun() -> update_nodes_async(Config, Pid) end),
    ok.

update_nodes_async(Config, Pid) ->
    Host = elysium_config:seed_node(Config),
    Query = <<"SELECT peer, tokens FROM system.peers;">>,
    lager:info("requesting peers to ~p", [Host]),
    case elysium_connection:one_shot_query(Config, Host, Query, one, trunc(timeout(Config) * 0.9)) of
        {error, _Error} -> lager:error("~p", [_Error]), ok;
        {ok, #rows{rows = Rows}} -> lager:debug("requesting peers result: ~p", [Rows]),
                                    Nodes = [to_host_port(N) || N <- Rows],
                                    Pid ! {update_nodes, [Host | Nodes]}
    end.

timeout(Config) ->
    elysium_config:request_peers_timeout(Config).

handle_node_change(New_Nodes, #state{nodes = Old_Nodes, config = _Config} = St) ->
    New_Nodes_Set = ordset:from_list(New_Nodes),
    Old_Nodes_Set = ordset:from_list(Old_Nodes),
    ok = case ordsets:substract(New_Nodes_Set, Old_Nodes_Set) of
             [] -> St;
             _  -> elysium_queue:node_change(New_Nodes),
                   St#state{nodes = New_Nodes}
         end.

to_host_port(Bin) ->
    [HostBin, PortBin] = binary:split(Bin, <<":">>),
    {binary_to_list(HostBin), binary_to_integer(PortBin)}.