%%%-----------------------------------------------------------------------
%%% Elysium_worker_sup restarts worker processes (which each
%%% contain a seestar Cassandra connection and a dictionary of
%%% prepared statements).
%%%-----------------------------------------------------------------------
-module(elysium_worker_sup).
-author('jay@duomark.com').

-behavior(supervisor).

%% External API
-export([start_link/0, init/1]).

-define(SUPER, ?MODULE).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUPER}, ?MODULE, {}).


%%%-----------------------------------------------------------------------
%%% Internal API
%%%-----------------------------------------------------------------------
-define(CHILD(__Mod), {__Mod, {__Mod, start_link, []},
                               permanent, 2000, worker, [__Mod]}).

-spec init({}) -> {ok, {{supervisor:strategy(), non_neg_integer(), non_neg_integer()},
                        [supervisor:child_spec()]}}.
init({}) ->
    Worker = ?CHILD(elysium_worker),
    {ok, {{simple_one_for_one, 10, 10}, [Worker]}}.
