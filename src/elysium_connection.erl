%%% @doc
%%%   An elysium_connection is a seestar_session gen_fsm. A
%%%   connection to Cassandra is opened during the initialization.
%%%   If this process ever crashes or ends, the connection is
%%%   closed and a replacement process can optionally be spawned.
%%%   Under normal operations, elysium_connection_sup will try
%%%   to create an immediate replacement for any failed processes.
%%%
%%%   Once a connection is successfully created, it is saved at
%%%   the end of an ets_buffer FIFO queue. The queue provides a
%%%   supply of Cassandra connection sessions, via the checkout/0
%%%   function. If none are available, the caller can decide how
%%%   to handle the situation appropriately.
%%%
%%%   Please ensure that connections are checked back in, possibly
%%%   even using a try / after, but note that if you link to the
%%%   checked out connection a process death of the caller can
%%%   safely take down the connection because the supervisor will
%%%   immediately spawn a replacement.
%%% @end
-module(elysium_connection).
-author('jay@duomark.com').

%% External API
-export([
         start_link/2, stop/1,
         checkin/1, checkout/0
        ]).

-define(MAX_CHECKOUT_RETRY, 20).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(string(), pos_integer()) -> {ok, pid()}.
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
start_link(Ip, Port)
  when is_list(Ip), is_integer(Port), Port > 0 ->
    case seestar_session:start_link(Ip, Port) of
        {ok, Pid} -> _ = checkin(Pid), {ok, Pid};
        Other     -> Other
    end.

-spec stop(pid()) -> ok.
%% @doc
%%   Stop an existing seestar_session.
%% @end
stop(Session_Id)
  when is_pid(Session_Id) ->
    seestar_session:stop(Session_Id).

-spec checkin(pid()) -> false | pos_integer().
%% @doc
%%   Checkin a seestar_session by putting it at the end of the
%%   available connection queue. Returns false if the connection
%%   process being checked in is dead, or the count of the number
%%   of connections available after the checkin.
%% @end
checkin(Session_Id)
  when is_pid(Session_Id) ->
    {ok, Queue} = application:get_env(elysium, cassandra_worker_queue),
    _ = is_process_alive(Session_Id)
        andalso ets_buffer:write_dedicated(Queue, Session_Id).
            
-spec checkout() -> pid() | none_available | {missing_ets_buffer, atom()}.
%% @doc
%%   Allocate a seestar_session to the caller by popping an entry
%%   from the front of the connection queue. This function either
%%   returns a live pid(), or none_available to indicate that all
%%   connections to Cassandra are currently checked out.
%% @end
checkout() ->
    {ok, Queue} = application:get_env(elysium, cassandra_worker_queue),
    fetch_pid_from_queue(Queue, 1).

%% Internal loop function to retry getting from the queue.
fetch_pid_from_queue(_Queue, ?MAX_CHECKOUT_RETRY) -> none_available;
fetch_pid_from_queue( Queue, Count) ->
    case ets_buffer:read_dedicated(Queue) of

        %% Give up if there are no connections available...
        [] -> none_available;

        %% Race condition with writer, try again...
        {missing_ets_data, Queue, _} ->
            fetch_pid_from_queue(Queue, Count+1);

        %% Return only a live pid, otherwise get the next one.
        [Session_Id] when is_pid(Session_Id) ->
            case is_process_alive(Session_Id) of
                false -> fetch_pid_from_queue(Queue, Count+1);
                true  -> Session_Id
            end;

        %% Somehow the connection buffer died, or something even worse!
        Error -> Error
    end.
