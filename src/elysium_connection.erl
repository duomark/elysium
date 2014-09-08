%%%-----------------------------------------------------------------------
%%% Each elysium_connection manages a seestar Cassandra connection.
%%%-----------------------------------------------------------------------
-module(elysium_connection).
-author('jay@duomark.com').

%% External API
-export([start_link/2, stop/1]).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
%% @doc
%%   Create a new seestar_session (a gen_server) and record its pid()
%%   in the elysium_connection ets_buffer. This FIFO queue serves up
%%   connections to all who ask for them, assuming that they will return
%%   the connection to the FIFO queue when they are done.
%%
%%   If a seestar_session fails, the elysium_connection_sup will start
%%   a new session. This function will again enter the pid() into the
%%   queue, replacing the crashed one that was removed from the queue
%%   when it was allocated for work.
%% @end
-spec start_link(string(), pos_integer()) -> {ok, pid()}.
start_link(Ip, Port)
when is_list(Ip), is_integer(Port), Port > 0 ->
    case seestar_session:start_link(Ip, Port) of
        {ok, Pid} ->
            {ok, Queue} = application:get_env(elysium, cassandra_worker_queue),
            _  = ets_buffer:write_dedicated(Queue, Pid),
            {ok, Pid};
        Other -> Other
    end.

%% @doc
%%   Stop an existing seestar_session.
%% @end
-spec stop(pid()) -> ok.
stop(Session_Id)
  when is_pid(Session_Id) ->
    seestar_session:stop(Session_Id).
