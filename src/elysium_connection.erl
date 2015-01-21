%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   An elysium_connection is an seestar_session gen_server. A
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
%%%   to handle the situation appropriately. Using the 'serial'
%%%   buffering strategy, pending requests are queued awaiting
%%%   a session which becomes free. Once queueing of requests
%%%   begins a session is not checked back in until the queue
%%%   has been worked off or the session decays and is killed.
%%%   Any replacement for a decayed session immediately attempts
%%%   to work off the backlog of pending requests.
%%%
%%%   Please ensure that connections are checked back in, possibly
%%%   even using a try / after, but note that if you link to the
%%%   checked out connection a process death of the caller can
%%%   safely take down the connection because the supervisor will
%%%   immediately spawn a replacement. The simplest technique for
%%%   managing sessions is to call with_connection/4,5 providing
%%%   the CQL commands to execute. The entire checkin/checkout
%%%   will be taken care of for you, follwing the buffer strategy
%%%   requested.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_connection).
-author('jay@duomark.com').

%% External API
-export([
         start_link/1,
         start_link/2,
         stop/1,
         one_shot_query/4,
         one_shot_query/5,
         get_buffer_strategy_module/1,
         with_connection/4,
         with_connection/5
        ]).

-include("elysium_types.hrl").
-type buffering() :: none | serial | parallel.
-export_type([buffering/0]).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}
                                       | {error, {cassandra_not_available,
                                                  [cassandra_node()]}}.
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
    Start_Delay   = elysium_config:max_restart_delay   (Config),
    ok = timer:sleep(elysium_random:random_int_up_to(Start_Delay)),
    start_channel(Config, Lb_Queue_Name, Max_Retries, -1, []).

%% @doc Restarts do not delay since they are stochastic already.
start_link(Config, restart) ->
    Lb_Queue_Name = elysium_config:load_balancer_queue (Config),
    Max_Retries   = elysium_config:checkout_max_retry  (Config),
    start_channel(Config, Lb_Queue_Name, Max_Retries, -1, []);

%% @doc start session on particular node
start_link(Config, {node, Node}) ->
    Max_Retries   = elysium_config:checkout_max_retry  (Config),
    start_channel(Config, {node, Node}, Max_Retries, -1, []).

-spec stop(connection_id()) -> ok.
%% @doc Stop an existing seestar_session.
stop(Connection_Id)
  when is_pid(Connection_Id) ->
    seestar_session:stop(Connection_Id).

-spec one_shot_query(config_type(), cassandra_node(), Query::iodata(), seestar:consistency())
                    -> {ok, seestar_result:result()} | {error, seestar_error:error()}.
%% @doc Connect, execute raw CQL and close a connection to Cassandra.
one_shot_query(Config, Node, Query, Consistency) ->
    one_shot_query(Config, Node, Query, Consistency, infinity).

-spec one_shot_query(config_type(), cassandra_node(), Query::iodata(), seestar:consistency(),
                     pos_integer() | infinity)
                    -> {ok, seestar_result:result()} | {error, seestar_error:error()}.
%% @doc Connect, execute raw CQL and close a connection to Cassandra with a timeout (in ms).
one_shot_query(Config, {Host, Port} = _Node, Query, Consistency, _Reply_Timeout)
  when is_list(Host),
       is_integer(Port), Port > 0 ->
    case get_one_shot_connection(Config, Host, Port) of
        {ok, Connection_Id} ->
            try   seestar_session:perform(Connection_Id, Query, Consistency)
            after stop(Connection_Id)
            end
        end.

-spec get_buffer_strategy_module(config_type()) -> {buffering(), buffering_strategy_module()}.
%% @doc Get the module corresponding to the configured buffering strategy.
get_buffer_strategy_module(Config) ->
    true = elysium_config:is_valid_config(Config),
    Buffering_Strategy = elysium_config:connection_buffering_strategy(Config),
    BS_Module = case Buffering_Strategy of
                    none     -> elysium_nobs;
                    parallel -> elysium_bs_parallel;
                    serial   -> elysium_bs_serial
                end,
    {Buffering_Strategy, BS_Module}.

-spec with_connection(config_type(), fun((pid(), Args, Consist) -> Result), Args, Consistency)
                     -> {error, no_db_connections | {atom(), any()}}
                            | Result when Args        :: [any()],
                                          Consist     :: seestar:consistency(),
                                          Consistency :: seestar:consistency(),
                                          Result      :: any().
%% @doc
%%   Obtain an active seestar_session and use it solely
%%   for the duration required to execute a fun which
%%   requires access to Cassandra.
%% @end
with_connection(Config, Session_Fun, Args, Consistency)
  when is_function(Session_Fun, 3), is_list(Args) ->
    {Buffering_Strategy, BS_Module} = get_buffer_strategy_module(Config),
    case elysium_buffering_strategy:checkout_connection(Config) of
        none_available ->
            buffer_bare_fun_call(Config, Session_Fun, Args, Consistency, Buffering_Strategy);
        {Node, Sid} when is_pid(Sid) ->
            case is_process_alive(Sid) of
                false -> with_connection(Config, Session_Fun, Args, Consistency);
                true  -> Reply_Timeout = elysium_config:request_reply_timeout(Config),
                         Query_Request = {bare_fun, Config, Session_Fun, Args, Consistency},
                         Reply = elysium_buffering_strategy:handle_pending_request
                                   (Config, BS_Module, 0, Reply_Timeout, Node, Sid, Query_Request),
                         handle_bare_fun_reply(Buffering_Strategy, Reply, ?MODULE, with_connection, Args)
            end
    end.

-spec with_connection(config_type(), module(), Fun::atom(), Args::[any()], seestar:consistency())
                     -> {error, no_db_connections | {atom(), any()}} | any().
%% @doc
%%   Obtain an active seestar_session and use it solely
%%   for the duration required to execute Mod:Fun
%%   which requires access to Cassandra.
%% @end
with_connection(Config, Mod, Fun, Args, Consistency)
  when is_atom(Mod), is_atom(Fun), is_list(Args) ->
    true = erlang:function_exported(Mod, Fun, 3),
    {Buffering_Strategy, BS_Module} = get_buffer_strategy_module(Config),
    case elysium_buffering_strategy:checkout_connection(Config) of
        none_available ->
            buffer_mod_fun_call(Config, Mod, Fun, Args, Consistency, Buffering_Strategy);
        {Node, Sid} when is_pid(Sid) ->
            case is_process_alive(Sid) of
                false -> with_connection(Config, Mod, Fun, Args, Consistency);
                true  -> Reply_Timeout = elysium_config:request_reply_timeout(Config),
                         Query_Request = {mod_fun, Config, Mod, Fun, Args, Consistency},
                         Reply = elysium_buffering_strategy:handle_pending_request
                                   (Config, BS_Module, 0, Reply_Timeout, Node, Sid, Query_Request),
                         handle_mod_fun_reply(Buffering_Strategy, Reply, Mod, Fun, Args)
            end
    end.


%%%-----------------------------------------------------------------------
%%% Internal connection support functions
%%%-----------------------------------------------------------------------

start_channel(_Config, _Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections)
  when Times_Tried >= Max_Retries ->
    {error, {cassandra_not_available, Attempted_Connections}};

start_channel( Config,  {node, {Ip, Port}=Node}, _Max_Retries, Times_Tried, Attempted_Connections) 
    when is_list(Ip), is_integer(Port), Port > 0 ->
    %% Attempt to connect to Cassandra node...
    Lb_Queue_Name   = elysium_config:load_balancer_queue (Config),
    try_connect(Config, Lb_Queue_Name, 0, Times_Tried, Attempted_Connections, Node);

start_channel( Config,  Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections) ->
    case elysium_lb_queue:checkout(Lb_Queue_Name) of
        %% Give up if there are no connections available...
        empty -> {error, {cassandra_not_available, Attempted_Connections}};
        %% Attempt to connect to Cassandra node...
        {value, {Ip, Port} = Node} when is_list(Ip), is_integer(Port), Port > 0 ->
            try_connect(Config, Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections, Node)
    end.

try_connect(Config, Lb_Queue_Name, Max_Retries, Times_Tried, Attempted_Connections, {Ip, Port} = Node) ->
    %% Send_Timeout    = elysium_config:send_timeout    (Config),
    Connect_Timeout = elysium_config:connect_timeout (Config),
    try seestar_session:start_link(Ip, Port,
                                   [
                                    %% {send_timeout,       Send_Timeout}
                                   ],
                                   [{connect_timeout, Connect_Timeout}]) of

        {ok, Connection_Id} = Connection when is_pid(Connection_Id) ->
            _ = elysium_buffering_strategy:create_connection(Config, Node, Connection_Id),
            Connection;

        %% If we fail, try again after recording attempt.
        Error ->
            Node_Failure = {Node, Error},
            start_channel(Config, Lb_Queue_Name, Max_Retries, Times_Tried+1,
                          [Node_Failure | Attempted_Connections])

    catch Err:Class ->
            Node_Failure = {Node, {Err, Class}},
            start_channel(Config, Lb_Queue_Name, Max_Retries, Times_Tried+1,
                          [Node_Failure | Attempted_Connections])

            %% Ensure that we get the Node checked back in.
    after _ = elysium_lb_queue:checkin(Lb_Queue_Name, Node)
    end.

-spec get_one_shot_connection(config_type(), string(), pos_integer()) -> {ok, connection_id()} | any().
get_one_shot_connection(Config, Host, Port) ->
    Connect_Timeout  = elysium_config:connect_timeout (Config),
    try seestar_session:start_link(Host, Port,
                                   [
                                    %% {send_timeout,       Send_Timeout}
                                   ],
                                   [{connect_timeout, Connect_Timeout}]) of
        {ok, Connection_Id} = Connection when is_pid(Connection_Id) -> Connection;
        Error       -> Error
    catch Err:Class -> {Err, Class}
    end.


%%%-----------------------------------------------------------------------
%%% Internal with_connection support functions
%%%-----------------------------------------------------------------------

buffer_bare_fun_call(_Config, _Session_Fun, _Args, _Consistency, none) -> {error, no_db_connections};
buffer_bare_fun_call( Config,  Session_Fun,  Args,  Consistency, Type) ->
    Reply = elysium_buffering_strategy:pend_request(Config, {bare_fun, Config, Session_Fun, Args, Consistency}),
    handle_bare_fun_reply(Type, Reply, ?MODULE, buffer_bare_fun_call, Args).

handle_bare_fun_reply(parallel, {Err_Type, _Err_Data} = Error, Mod, Fun, Args)
  when Err_Type =:= missing_ets_data;
       Err_Type =:= missing_ets_buffer;
       Err_Type =:= pending_ets_errors;
       Err_Type =:= pending_missing_data;
       Err_Type =:= session_ets_errors;
       Err_Type =:= session_missing_data;
       Err_Type =:= wait_for_session_timeout;
       Err_Type =:= worker_reply_error;
       Err_Type =:= worker_reply_timeout ->
    report_error(Error, Mod, Fun, Args);
handle_bare_fun_reply(serial, {Err_Type, _Err_Data} = Error, Mod, Fun, Args)
  when Err_Type =:= missing_ets_data;
       Err_Type =:= missing_ets_buffer;
       Err_Type =:= wait_for_session_timeout;
       Err_Type =:= worker_reply_error;
       Err_Type =:= worker_reply_timeout ->
    report_error(Error, Mod, Fun, Args);
handle_bare_fun_reply(_Buffering_Strategy, Reply, _Mod, _Fun, _Args) ->
    Reply.

buffer_mod_fun_call(_Config, _Mod, _Fun, _Args, _Consistency,     none) -> {error, no_db_connections};
buffer_mod_fun_call( Config,  Mod,  Fun,  Args,  Consistency, parallel) ->
    Reply = elysium_buffering_strategy:pend_request(Config, {mod_fun, Config, Mod, Fun, Args, Consistency}),
    handle_mod_fun_reply(parallel, Reply, Mod, Fun, Args);
buffer_mod_fun_call( Config,  Mod,  Fun,  Args,  Consistency, serial) ->
    Reply = elysium_buffering_strategy:pend_request(Config, {mod_fun, Config, Mod, Fun, Args, Consistency}),
    handle_mod_fun_reply(serial, Reply, Mod, Fun, Args).

handle_mod_fun_reply(parallel, {Err_Type, _Err_Data} = Error, Mod, Fun, Args)
  when Err_Type =:= missing_ets_data;
       Err_Type =:= missing_ets_buffer;
       Err_Type =:= pending_ets_errors;
       Err_Type =:= pending_missing_data;
       Err_Type =:= session_ets_errors;
       Err_Type =:= session_missing_data;
       Err_Type =:= wait_for_session_timeout;
       Err_Type =:= worker_reply_error;
       Err_Type =:= worker_reply_timeout ->
    report_error(Error, Mod, Fun, Args);
handle_mod_fun_reply(serial, {Err_Type, _Err_Data} = Error, Mod, Fun, Args)
  when Err_Type =:= missing_ets_data;
       Err_Type =:= missing_ets_buffer;
       Err_Type =:= wait_for_session_timeout;
       Err_Type =:= worker_reply_error;
       Err_Type =:= worker_reply_timeout ->
    report_error(Error, Mod, Fun, Args);
handle_mod_fun_reply(_Buffering_Strategy, Reply, _Mod, _Fun, _Args) ->
    Reply.

report_error(Error, Module, Function, Args) ->
    lager:error("~p:~p got ~p with ~p~n", [Module, Function, Error, Args]),
    {error, Error}.
