-module(elysium).
-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).


%%-------------------------------------------------------------------
%% ADMIN API
%%-------------------------------------------------------------------
-spec start() -> ok | {error, {already_started, ?MODULE}}.
start() ->
    application:start(?MODULE).

-spec stop() -> ok.
stop() ->
    application:stop(?MODULE).


%%-------------------------------------------------------------------
%% BEHAVIOUR CALLBACKS
%%-------------------------------------------------------------------
-spec start(any(), any()) -> {ok, pid()}.
start(_StartType, _StartArgs) -> 
    elysium_sup:start_link().

-spec stop(any()) -> no_return().
stop(_State) ->
    ok.
