%%%-----------------------------------------------------------------------
%%% Elysium consists of a gen_fsm to manage workers, and a supervisor
%%% which launches and restarts those workers. The worker pids are
%%% kept in a FIFO ets_buffer so that they can be retrieved and
%%% returned easily.
%%%-----------------------------------------------------------------------
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
start_link() ->
    supervisor:start_link({local, ?SUPER}, ?MODULE, {}).


%%%-----------------------------------------------------------------------
%%% Internal API
%%%-----------------------------------------------------------------------
-define(CHILD(__Mod), {__Mod, {__Mod, start_link, []},
                               permanent, 2000, worker, [__Mod]}).
-define(SUPER(__Mod, __Args), {__Mod,  {__Mod, start_link, __Args}, permanent, infinity, supervisor, [__Mod]}).

-spec init({}) -> {ok, {{supervisor:strategy(), non_neg_integer(), non_neg_integer()},
                        [supervisor:child_spec()]}}.
init({}) ->
    Procs = [
             ?CHILD(elysium_fsm),
             ?SUPER(elysium_worker_sup, [])
            ],

    {ok, {{rest_for_one, 10, 10}, Procs}}.
