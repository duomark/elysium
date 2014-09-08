%%%-----------------------------------------------------------------------
%%% Each elysium_worker manages a seestar Cassandra connection.
%%%-----------------------------------------------------------------------
-module(elysium_worker).
-author('jay@duomark.com').

%% External API
-export([start_link/0]).

-define(SUPER, ?MODULE).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
start_link() -> {ok, spawn_link(erlang, now, [])}.
