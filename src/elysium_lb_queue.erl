-module(elysium_lb_queue).

-behaviour(gen_server).

%% API exports.
-export([
         start_link/2,
         checkin/2,
         checkout/1,
         num_entries/1,
         is_empty/1,
         status/1,
         replace_items/2,
         start_session_on_new_nodes/3
        ]).

%% gen_server exports.
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-include("elysium_types.hrl").

-record(state, {
          queue = queue:new(),
          checkin_count  = 0 :: non_neg_integer(),
          checkout_count = 0 :: non_neg_integer(),
          nodes = [] :: [cassandra_node()],
          config             :: config_type()
         }).


%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-type prop() :: {checkin_count,  non_neg_integer()}
              | {checkout_count, non_neg_integer()}
              | {num_entries,    non_neg_integer()}.

-spec start_link(config_type(), queue_name()) -> {ok, pid()}.

-spec num_entries (queue_name()) -> non_neg_integer().
-spec is_empty    (queue_name()) -> boolean().
-spec status      (queue_name()) -> {status, [prop()]}.

start_link(Config, Queue_Name) -> gen_server:start_link({local, Queue_Name}, ?MODULE, {Config}, []).

checkin  (Queue_Name,  Queue_Data) -> gen_server:call(Queue_Name, {checkin, Queue_Data}).
checkout (Queue_Name)              -> gen_server:call(Queue_Name, checkout).
replace_items(Queue_Name, Queue_Data) -> gen_server:cast(Queue_Name, {replace, Queue_Data}).

num_entries (Queue_Name) -> gen_server:call(Queue_Name, num_entries).
is_empty    (Queue_Name) -> gen_server:call(Queue_Name, is_empty).
status      (Queue_Name) -> gen_server:call(Queue_Name, status).


%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

-spec init({config_type()}) -> {ok, #state{}}.

init({Config}) -> {ok, #state{config=Config}}.
terminate(_Reason, _St) -> ok.

handle_call({checkin, Queue_Data}, _From, #state{queue=Queue, checkin_count=Checkins, nodes = Nodes} = St) ->
    case {lists:member(Queue_Data, Nodes), queue:member(Queue_Data, Queue)} of
        {true, false} -> New_Queue = queue:in(Queue_Data, Queue),
                         New_Count = Checkins + 1,
                         {reply, queue:len(New_Queue), St#state{queue=New_Queue, checkin_count=New_Count}};
        _ -> %% Nodes changes already or already in queue (due to nodes change), ignore
             {reply, queue:len(Queue), St#state{checkin_count=Checkins+1}}
    end;


handle_call(checkout, _From, #state{queue=Queue, checkout_count=Checkouts} = St) ->
    {Value, New_Queue} = queue:out(Queue),
    New_Count = Checkouts + 1,
    {reply, Value, St#state{queue=New_Queue, checkout_count=New_Count}};

handle_call(num_entries, _From, #state{queue=Queue} = St) -> {reply, queue:len(Queue),      St};
handle_call(is_empty,    _From, #state{queue=Queue} = St) -> {reply, queue:is_empty(Queue), St};
handle_call(status,      _From, #state{queue=Queue, nodes=Nodes, checkin_count=In, checkout_count=Out} = St) ->
    Size  = queue:len(Queue),
    Props = [
             {checkin_count,    In},
             {checkout_count,  Out},
             {num_entries,    Size},
             {nodes, Nodes}
            ],
    {reply, {status, Props}, St};

handle_call(Request, _From, #state{} = St) -> {stop, {unexpected_call, Request}, St}.

%% start session on newly discovered nodes
start_session_on_new_nodes(New_Nodes, Old_Nodes, Config) ->
    Diff = [ Node || Node <- New_Nodes, lists:member(Node, Old_Nodes) =:= false],
    [start_session_on_node(Node, Config) || Node <- Diff],

start_session_on_node(Node, Config) ->   
    Supervisor_Pid = elysium_queue:get_connection_supervisor(),
    Max_Connections = elysium_config:session_max_count(Config),
    [ elysium_connection_sup:start_child(Supervisor_Pid, [Config, {node, Node}]) || 
        _ <- lists:seq(1, Max_Connections)
    ].

handle_cast({replace, {init, Seeds}}, #state{} = St) ->
    New_Queue = queue:new(),
    Updated_New_Queue = lists:foldl(fun(Item, Acc) -> queue:in(Item, Acc) end, New_Queue, Seeds),
    %% elysium_connection_sup is to start using seed node - so no need to start sessions here
    {noreply, St#state{queue=Updated_New_Queue, nodes=Seeds}};

handle_cast({replace, New_Items}, #state{nodes=Nodes, config=Config} = St) ->
    New_Queue = queue:new(),
    Updated_New_Queue = lists:foldl(fun(Item, Acc) -> queue:in(Item, Acc) end, New_Queue, New_Items),
    %% trigger new sessions on newly added nodes
    spawn(fun() -> start_session_on_new_nodes(New_Items, Nodes, Config) end),
    {noreply, St#state{queue=Updated_New_Queue, nodes=New_Items}};

handle_cast(Request, St) -> {stop, {unexpected_cast, Request}, St}.

%% Unused callbacks
handle_info(Info,    St) -> {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.
