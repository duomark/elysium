%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium contains a gen_fsm which manages an ets_buffer FIFO
%%%   queue and a set of Cassandra connection workers that are ordered
%%%   in the queue for allocation. A simple checkin / checkout API is
%%%   used to obtain a connection. If the checked out connection fails
%%%   for any reason, a supervisor will replace it and place the new
%%%   connection at the end of the queue.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_sup).
-author('jay@duomark.com').

-behavior(supervisor).

%% External API
-export([start_link/0, init/1]).

-define(SUPER, ?MODULE).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
-spec start_link() -> {ok, pid()}.
%% @doc
%%   Start the root supervisor. This is the one that should be started
%%   by any including supervisor. The config/sys.config provides a set
%%   of example parameters that the including application should specify.
%% @end
start_link() ->
    supervisor:start_link({local, ?SUPER}, ?MODULE, {}).


%%%-----------------------------------------------------------------------
%%% Internal API
%%%-----------------------------------------------------------------------
-define(CHILD(__Mod, __Args), {__Mod, {__Mod, start_link, __Args},
                               permanent, 2000, worker, [__Mod]}).
-define(SUPER(__Mod, __Args), {__Mod,  {__Mod, start_link, __Args},
                               permanent, infinity, supervisor, [__Mod]}).

-spec init({}) -> {ok, {{supervisor:strategy(), non_neg_integer(), non_neg_integer()},
                        [supervisor:child_spec()]}}.
%% @doc
%%   Starts the gen_fsm which owns the Cassandra connection queue,
%%   and the supervisor of all Cassandra connections. The two are
%%   rest_for_one so that the queue is guaranteed to exist before
%%   any connections are created.
%% @end
init({}) ->
    {ok, Ip}          = application:get_env(elysium, cassandra_ip),
    {ok, Port}        = application:get_env(elysium, cassandra_port),
    {ok, Num_Workers} = application:get_env(elysium, cassandra_worker_max_count),
    {ok, Queue_Name}  = elysium_queue:configured_name(),
    Procs = [
             ?CHILD(elysium_queue,          [Queue_Name]),
             ?SUPER(elysium_connection_sup, [Ip, Port, Num_Workers])
            ],

    {ok, {{rest_for_one, 10, 10}, Procs}}.
