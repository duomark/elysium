%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   An elysium_connection is a seestar_session gen_server. A
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
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_connection).
-author('jay@duomark.com').

%% External API
-export([
         start_link/2, stop/1,
         with_connection/3,
         with_connection/4
        ]).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(string(), pos_integer()) -> {ok, pid()} | {error, tuple()}.
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
        {ok, Pid} ->
            {ok, Queue_Name} = elysium_queue:configured_name(),
            _ = elysium_queue:checkin(Queue_Name, Pid),
            {ok, Pid};
        {error, {connection_error, econnrefused}} ->
            {error, {cassandra_not_available, [{ip, Ip}, {port, Port}]}};
        Other -> Other
    end.

-spec stop(pid()) -> ok.
%% @doc
%%   Stop an existing seestar_session.
%% @end
stop(Session_Id)
  when is_pid(Session_Id) ->
    seestar_session:stop(Session_Id).

-type ets_buffer_name() :: atom().
-spec with_connection(ets_buffer_name(), fun((pid(), Args) -> Result), Args)
                     -> {error, no_db_connections}
                            | Result when Args   :: [any()],
                                          Result :: any().
%% @doc
%%   Obtain an active seestar_session and use it solely
%%   for the duration required to execute a fun which
%%   requires access to Cassandra.
%% @end
with_connection(Queue_Name, Session_Fun, Args)
  when is_function(Session_Fun, 2), is_list(Args) ->
    case elysium_queue:checkout(Queue_Name) of
        none_available       -> {error, no_db_connections};
        Sid when is_pid(Sid) ->
            try    Session_Fun(Sid, Args)
            after  _ = elysium_queue:checkin(Queue_Name, Sid)
            end
    end.

-spec with_connection(ets_buffer_name(), module(), Fun::atom(), Args::[any()])
                     -> {error, no_db_connections} | any().
%% @doc
%%   Obtain an active seestar_session and use it solely
%%   for the duration required to execute Mod:Fun
%%   which requires access to Cassandra.
%% @end
with_connection(Queue_Name, Mod, Fun, Args)
  when is_atom(Mod), is_atom(Fun), is_list(Args) ->
    case elysium_queue:checkout(Queue_Name) of
        none_available       -> {error, no_db_connections};
        Sid when is_pid(Sid) ->
            try    Mod:Fun(Sid, Args)
            after  _ = elysium_queue:checkin(Queue_Name, Sid)
            end
    end.
