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
%%%   supply of Cassandra connection sessions, via the checkout/1
%%%   function. If none are available, the caller can decide how
%%%   to handle the situation appropriately.
%%%
%%%   Please ensure that connections are checked back in, possibly
%%%   even using a try / after, but note that if you link to the
%%%   checked out connection a process death of the caller can
%%%   safely take down the connection because the supervisor will
%%%   immediately spawn a replacement. The simplest technique is
%%%   to call with_connection/3,4
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_connection).
-author('jay@duomark.com').

%% External API
-export([
         start_link/1,
         stop/1,
         with_connection/5,
         with_connection/6
        ]).

-include("elysium_types.hrl").
-type buffering() :: none | overload | serial.
-export_types([buffering/0]).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-type node_attempt() :: {Ip::string(), Port::pos_integer()}.
-spec start_link(config_type()) -> {ok, pid()}
                                       | {error, {cassandra_not_available,
                                                  [node_attempt()]}}.
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
start_link(Config) ->
    Lb_Queue_Name = elysium_config:load_balancer_queue (Config),
    Max_Retries   = elysium_config:checkout_max_retry  (Config),
    Restart_Delay = elysium_config:max_restart_delay   (Config),
    ok = timer:sleep(elysium_random:random_int_up_to(Restart_Delay)),
    start_session(Config, Lb_Queue_Name, Max_Retries, -1, []).

start_session(_Config, _Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections)
  when Times_Tried >= Max_Retries ->
    {error, {cassandra_not_available, Attempted_Connections}};

start_session( Config,  Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections) ->
    case ets_buffer:read_dedicated(Lb_Queue_Name) of

        %% Give up if there are no connections available...
        [] -> {error, {cassandra_not_available, Attempted_Connections}};

        %% Race condition with another user, try again...
        %% (Internally, ets_buffer calls erlang:yield() when this happens)
        {missing_ets_data, Lb_Queue_Name, _} ->
            start_session(Config, Lb_Queue_Name, Max_Retries, Times_Tried+1, Attempted_Connections);

        %% Attempt to connect to Cassandra node...
        [{Ip, Port} = Node] when is_list(Ip), is_integer(Port), Port > 0 ->
            try_connect(Config, Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections, Node);

        %% Somehow the load balancer queue died, or something even worse!
        Error -> Error
    end.

try_connect(Config, Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections, {Ip, Port} = Node) ->
    Connect_Timeout = elysium_config:connect_timeout(Config),
    try seestar_session:start_link(Ip, Port, [], [{connect_timeout, Connect_Timeout}]) of
        {ok, Pid} = Session when is_pid(Pid) ->
            _ = elysium_bs_serial:checkin_connection(Config, Pid),
            Session;

        %% If we fail, try again after recording attempt.
        Error ->
            Node_Failure = {Node, Error},
            start_session(Config, Lb_Queue_Name, Max_Retries, Times_Tried+1, [Node_Failure | Attempted_Connections])

    catch Error:Class ->
            Node_Failure = {Node, {Error, Class}},
            start_session(Config, Lb_Queue_Name, Max_Retries, Times_Tried+1, [Node_Failure | Attempted_Connections])

            %% Ensure that we get the Node checked back in.
    after _ = ets_buffer:write_dedicated(Lb_Queue_Name, Node)
    end.

-spec stop(pid()) -> ok.
%% @doc
%%   Stop an existing seestar_session.
%% @end
stop(Session_Id)
  when is_pid(Session_Id) ->
    seestar_session:stop(Session_Id).


-spec with_connection(config_type(), fun((pid(), Args, Consist) -> Result), Args, Consistency, buffering())
                     -> {error, no_db_connections}
                            | Result when Args        :: [any()],
                                          Consist     :: seestar:consistency(),
                                          Consistency :: seestar:consistency(),
                                          Result      :: any().
%% @doc
%%   Obtain an active seestar_session and use it solely
%%   for the duration required to execute a fun which
%%   requires access to Cassandra.
%% @end
with_connection(Config, Session_Fun, Args, Consistency, Buffering_Strategy)
  when is_function(Session_Fun, 3), is_list(Args) ->
    case elysium_bs_serial:checkout_connection(Config) of
        none_available -> buffer_bare_fun_call(Config, Session_Fun, Args, Consistency, Buffering_Strategy);
        Sid when is_pid(Sid) ->
            try    Session_Fun(Sid, Args, Consistency)
            after  _ = elysium_bs_serial:checkin_connection(Config, Sid)
            end
    end.

buffer_bare_fun_call(_Config, _Session_Fun, _Args, _Consistency, none)   -> {error, no_db_connections};
buffer_bare_fun_call( Config,  Session_Fun,  Args,  Consistency, serial) ->
    elysium_bs_serial:pend_request(Config, {bare_fun, Config, Session_Fun, Args, Consistency}).


-spec with_connection(config_type(), module(), Fun::atom(), Args::[any()], seestar:consistency(), buffering())
                     -> {error, no_db_connections} | any().
%% @doc
%%   Obtain an active seestar_session and use it solely
%%   for the duration required to execute Mod:Fun
%%   which requires access to Cassandra.
%% @end
with_connection(Config, Mod, Fun, Args, Consistency, Buffering_Strategy)
  when is_atom(Mod), is_atom(Fun), is_list(Args) ->
    true = erlang:function_exported(Mod, Fun, 3),
    case elysium_bs_serial:checkout_connection(Config) of
        none_available       -> buffer_mod_fun_call(Config, Mod, Fun, Args, Consistency, Buffering_Strategy);
        Sid when is_pid(Sid) ->
            try    Mod:Fun(Sid, Args, Consistency)
            after  _ = elysium_bs_serial:checkin_connection(Config, Sid)
            end
    end.

buffer_mod_fun_call(_Config, _Mod, _Fun, _Args, _Consistency, none)   -> {error, no_db_connections};
buffer_mod_fun_call( Config,  Mod,  Fun,  Args,  Consistency, serial) ->
    elysium_bs_serial:pend_request(Config, {mod_fun, Config, Mod, Fun, Args, Consistency}).
