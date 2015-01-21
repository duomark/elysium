%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   An example application is provided to start the root
%%%   supervisor. This is insufficient to run a real application,
%%%   but provides a quick way to poke around the supervisor
%%%   hierarchy after cloning and building.
%%%
%%%   The proper way to use this library is to make it an
%%%   included_application within a larger application. The
%%%   elysium_sup supervisor should be started and supervised
%%%   by another supervisor within the including application.
%%%   There are no startup phases which need be coordinated.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium).
-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).


%%-------------------------------------------------------------------
%% ADMIN API
%%-------------------------------------------------------------------
-spec start() -> ok | {error, {already_started, ?MODULE}}.
%% @doc
%%   Erl shell prompt starting of the example application.
%% @end
start() ->
    application:start(?MODULE).

-spec stop() -> ok.
%% @doc
%%   Erl shell prompt stopping of the example application.
stop() ->
    application:stop(?MODULE).


%%-------------------------------------------------------------------
%% BEHAVIOUR CALLBACKS
%%-------------------------------------------------------------------
-spec start(any(), any()) -> {ok, pid()}.
%% @doc
%%   Start the example application using 'erl -s elysium'.
%% @end
start(_StartType, _StartArgs) -> 
    elysium_sup:start_link({config_mod, elysium_default_config}).

-spec stop(any()) -> no_return().
%% @doc
%%   Stop the example application when started from erl command line.
%% @end
stop(_State) ->
    ok.
