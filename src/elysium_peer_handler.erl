-module(elysium_peer_handler).
-author('hernan@inakanetworks.com').

-behaviour(gen_server).

%% API exports.
-export([
         start_link/1,
         get_nodes/0,
         update_nodes/0,
         update_nodes/1,
         update_config/1
        ]).

%% gen_server exports.
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-include("elysium_types.hrl").
-include_lib("seestar/src/seestar_messages.hrl").

-record(state, {
          nodes = []               :: [cassandra_node()],
          config = undefined       :: undefined | config_type(),
          timer = undefined        :: undefined | timer:tref(),
          curr_request = undefined :: undefined | {pid(), reference()}
         }).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
%% @doc 
start_link(Config) -> gen_server:start_link({local, ?MODULE}, ?MODULE, {Config}, []).

-spec get_nodes() -> [cassandra_node()].
%% @doc Returns the list of nodes, this is updated every minute
get_nodes() -> gen_server:call(?MODULE, {get_nodes}).

-spec update_nodes() -> ok.
%% @doc Updates the node list, it is automatically called, but can be manually invoked
update_nodes() -> update_nodes(true).

-spec update_nodes(boolean()) -> ok.
%% @doc Updates the node list, if forced, it will cancel any pending requests and start a new one
update_nodes(_Force = true)  -> gen_server:cast(?MODULE, {update_nodes, forced});
update_nodes(_Force = false) -> gen_server:cast(?MODULE, {update_nodes, normal}).

-spec update_config(config_type()) -> [binary()].
%% @doc Updates the configuration used when re requesting the nodes
update_config(Config) -> gen_server:call(?MODULE, {update_config, Config}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

-spec init({config_type()}) -> {ok, #state{}}.

init({Config}) -> 
    %% start with seed node
    Host = elysium_config:seed_node(Config),
    Lb_Queue_Name = elysium_config:load_balancer_queue(Config),
    elysium_lb_queue:replace_items(Lb_Queue_Name, {init, [Host]}),
    {ok, Timer} = timer:apply_interval(timeout(Config), ?MODULE, update_nodes, []),
    Pid = self(),
    Curr_Request = spawn_monitor(fun() -> update_nodes(Config, Pid) end),
    {ok, #state{config = Config, timer = Timer, curr_request = Curr_Request, nodes=[Host]}}.

terminate(_Reason, _St) -> ok.

handle_call({get_nodes}, _From, #state{nodes = Nodes} = St) ->
    {reply, Nodes, St};
handle_call({update_config, New_Config}, _From, St) ->
    {reply, ok, St#state{config = New_Config}}.

handle_info({update_nodes, Caller, New_Nodes}, #state{curr_request = {Caller, _}} = St) ->
    New_State = handle_node_change(New_Nodes, St),
    {noreply, New_State#state{curr_request = undefined}};
handle_info({'DOWN', Ref, _, _, _}, #state{curr_request = {_, Ref}} = St) ->
    {noreply, St#state{curr_request = undefined}};
handle_info({'DOWN', _, _, _, _}, St) ->
    {noreply, St};
handle_info(_Whatever, St) ->
    {noreply, St}.

handle_cast({update_nodes, _}, #state{curr_request = undefined, config = Config} = St) ->
    Pid = self(),
    Curr_Request = spawn_monitor(fun() -> update_nodes(Config, Pid) end),
    {noreply, St#state{curr_request = Curr_Request}};
handle_cast({update_nodes, forced}, #state{curr_request = {Curr_Request, _}, config = Config} = St) ->
    exit(Curr_Request, kill),
    Pid = self(),
    New_Request = spawn_monitor(fun() -> update_nodes(Config, Pid) end),
    {noreply, St#state{curr_request = New_Request}};
handle_cast({update_nodes, _}, St) -> %% irgnore others 
    {noreply, St}.

%% Unused callbacks

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%% -------------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------------

update_nodes(Config, Pid) ->
    Host = elysium_config:seed_node(Config),
    Query = <<"SELECT peer FROM system.peers;">>,
    lager:info("requesting peers to ~p", [Host]),
    case elysium_connection:one_shot_query(Config, Host, Query, one, trunc(timeout(Config) * 0.9)) of
        {error, _Error} -> lager:error("~p", [_Error]);
        {ok, #rows{rows = []}}   -> lager:warning("update nodes returned empty list");
        {ok, #rows{rows = Rows}} -> lager:debug("requesting peers result: ~p", [Rows]),
                                    Nodes = [to_host_port(N, Config) || N <- Rows],
                                    Pid ! {update_nodes, self(), sets:to_list(sets:from_list([Host | Nodes]))},
                                    ok
    end.

timeout(Config) ->
    elysium_config:request_peers_frequency(Config).

handle_node_change(New_Nodes, #state{nodes = Old_Nodes, config = Config} = St) ->
    New_Nodes_Set = ordsets:from_list(New_Nodes),
    Old_Nodes_Set = ordsets:from_list(Old_Nodes),
    case New_Nodes_Set =:= Old_Nodes_Set of
        true  -> St;
        false -> Lb_Queue_Name = elysium_config:load_balancer_queue(Config),
                 elysium_lb_queue:replace_items(Lb_Queue_Name, New_Nodes),
                 St#state{nodes = New_Nodes}
    end.

to_host_port([Term], Config) ->
    Host = inet_parse:ntoa(Term),
    Port = elysium_config:default_port(Config),
    {Host, Port}.
