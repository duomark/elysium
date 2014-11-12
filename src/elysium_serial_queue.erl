%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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

%% API exports.
-export([
         start_link/1,
         checkin/2,       checkout/1,
         checkin_count/1, checkout_count/1,
         num_entries/1,   is_empty/1
        ]).

%% gen_server exports.
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-include("elysium_types.hrl").

-record(state, {
          queue = queue:new(),
          checkin_count  = 0 :: non_neg_integer(),
          checkout_count = 0 :: non_neg_integer()
         }).


%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-type queue_name() :: atom().
-type queue_data() :: any().

-spec start_link(queue_name()) -> {ok, pid()}.

-spec checkin  (queue_name(), queue_data()) -> ok.
-spec checkout (queue_name())               -> {value, queue_data()} | empty.

-spec checkin_count  (queue_name()) -> non_neg_integer().
-spec checkout_count (queue_name()) -> non_neg_integer().

-spec num_entries (queue_name()) -> non_neg_integer().
-spec is_empty    (queue_name()) -> boolean().

start_link(Queue_Name) -> gen_server:start_link({local, Queue_Name}, ?MODULE, {}, []).

checkin  (Queue_Name,  Queue_Data) -> gen_server:call(Queue_Name, {checkin, Queue_Data}).
checkout (Queue_Name)              -> gen_server:call(Queue_Name, checkout).

checkin_count  (Queue_Name) -> gen_server:call(Queue_Name, checkin_count).
checkout_count (Queue_Name) -> gen_server:call(Queue_Name, checkout_count).

num_entries (Queue_Name) -> gen_server:call(Queue_Name, num_entries).
is_empty    (Queue_Name) -> gen_server:call(Queue_Name, is_empty).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

-spec init({}) -> {ok, #state{}}.

init({}) -> {ok, #state{}}.
terminate(_Reason, _St) -> ok.

handle_call({checkin, Queue_Data}, _From, #state{queue=Queue, checkin_count=Checkins} = St) ->
    New_Queue = queue:in(Queue, Queue_Data),
    New_Count = Checkins + 1,
    {reply, ok, St#state{queue=New_Queue, checkin_count=New_Count}};

handle_call(checkout, _From, #state{queue=Queue, checkout_count=Checkouts} = St) ->
    {Value, New_Queue} = queue:out(Queue),
    New_Count = Checkouts + 1,
    {reply, Value, St#state{queue=New_Queue, checkin_count=New_Count}};

handle_call(checkin_count,  _From, #state{checkin_count=Count}  = St) -> {reply, Count, St};
handle_call(checkout_count, _From, #state{checkout_count=Count} = St) -> {reply, Count, St};
handle_call(num_entries,    _From, #state{queue=Queue}          = St) -> {reply, queue:len(Queue),      St};
handle_call(is_empty,       _From, #state{queue=Queue}          = St) -> {reply, queue:is_empty(Queue), St};

handle_call(Request,        _From, #state{} = St) -> {stop, {unexpected_call, Request}, St}.


%% Unused callbacks

handle_cast(Request, St) -> {stop, {unexpected_cast, Request}, St}.
handle_info(Info,    St) -> {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.
