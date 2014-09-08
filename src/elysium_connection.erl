%%%-----------------------------------------------------------------------
%%% Each elysium_connection manages a seestar Cassandra connection.
%%%-----------------------------------------------------------------------
-module(elysium_connection).
-author('jay@duomark.com').

%% External API
-export([start_link/2, stop/1]).

-define(SUPER, ?MODULE).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
-spec start_link(string(), pos_integer()) -> {ok, pid()}.
start_link(Ip, Port)
when is_list(Ip), is_integer(Port), Port > 0 ->
    seestar_session:start_link(Ip, Port).

-spec stop(pid()) -> ok.
stop(Session_Id)
  when is_pid(Session_Id) ->
    seestar_session:stop(Session_Id).
