%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_serial_queue is a gen_server with a queue as internal state
%%%   to manage doling out the connections
%%%
%%% @since 0.1.6i
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_serial_queue).

-behaviour(gen_server).

%%% API exports.
-export([
         start_link/1,
         checkin/2,
         checkout/1,
         num_entries/1,
         is_empty/1,
         status/1,
         status_reset/1
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
          checkin_count  = 0 :: checkin_count(),
          checkout_count = 0 :: checkout_count()
         }).


%%% -------------------------------------------------------------------------
%%% API
%%% -------------------------------------------------------------------------

-type queue_item() :: any().
-type queue_len()  :: non_neg_integer().

-type prop() :: {checkin_count,  checkin_count()}
              | {checkout_count, checkout_count()}
              | {num_entries,    queue_len()}.

-spec start_link   (queue_name()) -> {ok, pid()}.

-spec checkin      (queue_name(), queue_item()) -> checkin_count().
-spec checkout     (queue_name())               -> {value, queue_item()} | empty.

-spec num_entries  (queue_name()) -> queue_len().
-spec is_empty     (queue_name()) -> boolean().
-spec status       (queue_name()) -> {status, [prop()]}.
-spec status_reset (queue_name()) -> {status, [prop()]}.

start_link   (Queue_Name) -> gen_server:start_link({local, Queue_Name}, ?MODULE, {}, []).

checkin      (Queue_Name, Queue_Item) -> gen_server:call(Queue_Name, {checkin, Queue_Item}).
checkout     (Queue_Name)             -> gen_server:call(Queue_Name, checkout).

num_entries  (Queue_Name) -> gen_server:call(Queue_Name, num_entries).
is_empty     (Queue_Name) -> gen_server:call(Queue_Name, is_empty).
status       (Queue_Name) -> gen_server:call(Queue_Name, status).
status_reset (Queue_Name) -> gen_server:call(Queue_Name, status_reset).


%%% -------------------------------------------------------------------------
%%% gen_server callback functions
%%% -------------------------------------------------------------------------

-type from() :: {pid(), reference()}.

-spec init({}) -> {ok, #state{}}.
-spec handle_call({checkin, queue_item()}, from(), #state{}) -> {reply, queue_len(),  #state{}};
                 ( checkout,               from(), #state{}) -> {reply, queue_item(), #state{}};
                 ( num_entries,            from(), #state{}) -> {reply, queue_len(),  #state{}};
                 ( status_reset,           from(), #state{}) -> {reply, prop(),       #state{}};
                 ( is_empty,               from(),    State) -> {reply, boolean(),       State}
                                                                        when State :: #state{};
                 ( status,                 from(),    State) -> {reply, prop(),          State}
                                                                        when State :: #state{}.
                          
init({}) -> {ok, #state{}}.
terminate(_Reason, _St) -> ok.

handle_call({checkin, Queue_Item}, _From, #state{queue=Queue, checkin_count=Checkins} = St) ->
    New_Count = Checkins + 1,
    New_Queue = queue:in(Queue_Item, Queue),
    {reply, queue:len(New_Queue), St#state{queue=New_Queue, checkin_count=New_Count}};

handle_call(checkout, _From, #state{queue=Queue, checkout_count=Checkouts} = St) ->
    New_Count = Checkouts + 1,
    {Value, New_Queue} = queue:out(Queue),
    {reply, Value, St#state{queue=New_Queue, checkout_count=New_Count}};

handle_call(num_entries,  _From, #state{queue=Queue} = St) -> {reply, queue:len(Queue),      St};
handle_call(is_empty,     _From, #state{queue=Queue} = St) -> {reply, queue:is_empty(Queue), St};
handle_call(status,       _From, #state{} = St) ->
    Props = get_status_counts(St),
    {reply, {status, Props}, St};

handle_call(status_reset, _From, #state{} = St) ->
    Props = get_status_counts(St),
    {reply, {status, Props}, St#state{checkin_count=0, checkout_count=0}};

handle_call(Request, _From, #state{} = St) -> {stop, {unexpected_call, Request}, St}.


%%% Unused callbacks
handle_cast(Request, St) -> {stop, {unexpected_cast, Request}, St}.
handle_info(Info,    St) -> {stop, {unexpected_info,    Info}, St}.
code_change(_OldVsn, St, _Extra) -> {ok, St}.


%%% Support functions
get_status_counts(#state{queue=Queue, checkin_count=In, checkout_count=Out}) ->
    Size  = queue:len(Queue),
    [
     {checkin_count,    In},
     {checkout_count,  Out},
     {num_entries,    Size}
    ].
