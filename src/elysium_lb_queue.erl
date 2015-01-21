%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_lb_queue is a gen_server with a queue as internal state
%%%   to be used when establishing connections in round-robin fashion.
%%%
%%% @since 0.1.6i
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_lb_queue).

-behaviour(gen_server).

%%% API exports.
-export([
         start_link/2,
         checkin/2,
         checkout/1,
         num_entries/1,
         is_empty/1,
         status/1,
         status_reset/1,
         replace_items/2,
         start_session_on_new_nodes/3
        ]).

%%% gen_server exports.
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-include("elysium_types.hrl").

-type checkin_count()  :: non_neg_integer().
-type checkout_count() :: non_neg_integer().

-record(state, {
          queue = queue:new(),
          checkin_count  = 0  :: checkin_count(),
          checkout_count = 0  :: checkout_count(),
          nodes          = [] :: [cassandra_node()],
          config              :: config_type()
         }).


%%% -------------------------------------------------------------------------
%%% API
%%% -------------------------------------------------------------------------

-type queue_item() :: any().
-type queue_len()  :: non_neg_integer().

-type prop() :: {nodes,         [cassandra_node()]}
              | {checkin_count,  checkin_count()}
              | {checkout_count, checkout_count()}
              | {num_entries,    queue_len()}.

-spec start_link (config_type(), queue_name()) -> {ok, pid()}.

-spec replace_items (queue_name(), {init, host_list()}) -> ok;
                    (queue_name(),         host_list()) -> ok.

-spec checkin       (queue_name(),  queue_item() ) -> checkin_count().
-spec checkout      (queue_name())                 -> {value, queue_item()} | empty.

-spec num_entries   (queue_name()) -> queue_len().
-spec is_empty      (queue_name()) -> boolean().
-spec status        (queue_name()) -> {status, [prop()]}.
-spec status_reset  (queue_name()) -> {status, [prop()]}.

start_link (Config, Queue_Name) -> gen_server:start_link({local, Queue_Name}, ?MODULE, {Config}, []).

replace_items (Queue_Name, Queue_Items) -> gen_server:cast(Queue_Name, {replace, Queue_Items}).
checkin       (Queue_Name, Queue_Item)  -> gen_server:call(Queue_Name, {checkin, Queue_Item}).
checkout      (Queue_Name)              -> gen_server:call(Queue_Name, checkout).

num_entries   (Queue_Name) -> gen_server:call(Queue_Name, num_entries).
is_empty      (Queue_Name) -> gen_server:call(Queue_Name, is_empty).
status        (Queue_Name) -> gen_server:call(Queue_Name, status).
status_reset  (Queue_Name) -> gen_server:call(Queue_Name, status_reset).


%%% -------------------------------------------------------------------------
%%% gen_server callback functions
%%% -------------------------------------------------------------------------

-type from() :: {pid(), reference()}.

-spec init({config_type()}) -> {ok, #state{}}.
-spec handle_call({checkin, queue_item()}, from(), #state{}) -> {reply, queue_len(),  #state{}};
                 ( checkout,               from(), #state{}) -> {reply, queue_item(), #state{}};
                 ( num_entries,            from(), #state{}) -> {reply, queue_len(),  #state{}};
                 ( status_reset,           from(), #state{}) -> {reply, prop(),       #state{}};
                 ( is_empty,               from(),    State) -> {reply, boolean(),       State}
                                                                        when State :: #state{};
                 ( status,                 from(),    State) -> {reply, prop(),          State}
                                                                        when State :: #state{}.

init({Config}) -> {ok, #state{config=Config}}.
terminate(_Reason, _St) -> ok.

handle_call({checkin, Queue_Item}, _From, #state{queue=Queue, checkin_count=Checkins, nodes=Nodes} = St) ->
    case {lists:member(Queue_Item, Nodes), queue:member(Queue_Item, Queue)} of
        {true, false} ->  % Expected node and not already checked in
            New_Count = Checkins + 1,
            New_Queue = queue:in(Queue_Item, Queue),
            {reply, queue:len(New_Queue), St#state{queue=New_Queue, checkin_count=New_Count}};
        _ -> %% Nodes changes or already in queue (due to nodes change), ignore
            {reply, queue:len(Queue), St#state{checkin_count=Checkins+1}}
    end;

handle_call(checkout, _From, #state{queue=Queue, checkout_count=Checkouts} = St) ->
    New_Count = Checkouts + 1,
    {Value, New_Queue} = queue:out(Queue),
    {reply, Value, St#state{queue=New_Queue, checkout_count=New_Count}};

handle_call(num_entries, _From, #state{queue=Queue} = St) -> {reply, queue:len(Queue),      St};
handle_call(is_empty,    _From, #state{queue=Queue} = St) -> {reply, queue:is_empty(Queue), St};
handle_call(status,      _From, #state{} = St) ->
    Props = get_status_counts(St),
    {reply, {status, Props}, St};

handle_call(status_reset, _From, #state{} = St) ->
    Props = get_status_counts(St),
    {reply, {status, Props}, St#state{checkin_count=0, checkout_count=0}};

handle_call(Request, _From, #state{} = St) -> {stop, {unexpected_call, Request}, St}.


%%% Cast callbacks
handle_cast({replace, {init, Seeds}}, #state{} = St) ->
    New_Queue = queue:from_list(Seeds),
    %% elysium_connection_sup is to start using seed node - so no need to start sessions here
    {noreply, St#state{queue=New_Queue, nodes=Seeds}};

handle_cast({replace, New_Items}, #state{nodes=Nodes, config=Config} = St) ->
    New_Queue = queue:from_list(New_Items),
    %% trigger new sessions on newly added nodes
    spawn(fun() -> start_session_on_new_nodes(New_Items, Nodes, Config) end),
    {noreply, St#state{queue=New_Queue, nodes=New_Items}};

handle_cast(Request, St) -> {stop, {unexpected_cast, Request}, St}.


%%% Unused callbacks
handle_info(Info,    St) -> {stop, {unexpected_info, Info}, St}.
code_change(_OldVsn, St, _Extra) -> {ok, St}.


%%% Support functions
get_status_counts(#state{queue=Queue, nodes=Nodes, checkin_count=In, checkout_count=Out}) ->
    Size  = queue:len(Queue),
    [
     {checkin_count,    In},
     {checkout_count,  Out},
     {num_entries,    Size},
     {nodes,         Nodes}
    ].

%%% start session on newly discovered nodes
start_session_on_new_nodes(New_Nodes, Old_Nodes, Config) ->
    Diff = [ Node || Node <- New_Nodes, not lists:member(Node, Old_Nodes)],
    [start_session_on_node(Node, Config) || Node <- Diff].

start_session_on_node(Node, Config) ->   
    Supervisor_Pid  = elysium_queue:get_connection_supervisor(),
    Max_Connections = elysium_config:session_max_count(Config),
    [ elysium_connection_sup:start_child(Supervisor_Pid, [Config, {node, Node}]) || 
        _ <- lists:seq(1, Max_Connections)
    ].
