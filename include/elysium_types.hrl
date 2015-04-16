%% Two ways of specifying configuration parameters, designed for high concurrency.
-type config_type() :: config_app_config
                     | {config_mod, module()}
                     | {vbisect,    vbisect:bindict()}.

%% Buffering module names are used for telemetry.
-type buffering_strategy_module() :: module().

%% There are three queues used by elsyium.
-type queue_name()             :: ets_buffer:buffer_name() | atom().
-type lb_queue_name()          :: queue_name().    % Cassandra node load balancer queue
-type connection_queue_name()  :: queue_name().    % Cassandra open connections queue
-type requests_queue_name()    :: queue_name().    % Pending requests queue

-type audit_ets_name()         :: atom().          % Audit ets table for telemetry data

-type cassandra_node()   :: {Ip_Or_Hostname::inet:hostname(), Port::inet:port_number()}.   % Cassandra node
-type host_list()        :: [cassandra_node()].    % Host list used by load balancer

-type timeout_in_ms()    :: pos_integer().         % In milliseconds
-type max_retries()      :: non_neg_integer().
-type decay_prob()       :: non_neg_integer().     % Chances in 1B of connection death

-type request_peers_frequency() :: timeout_in_ms().

%% Currently Cassandra connections are seestar_sessions.
-type connection_id() :: pid().                    % Live process holding socket to Cassandra.
-type fun_request()   :: fun((connection_id(), [any()], seestar:consistency()) -> [any()]).
-type query_request() :: {bare_fun, config_type(), fun_request(),    [any()], seestar:consistency()}
                       | {mod_fun,  config_type(), module(), atom(), [any()], seestar:consistency()}.

-type cassandra_connection()  :: {cassandra_node(), connection_id()}.
-type connection_baton()      :: {{connection_id(), reference}, erlang:timestamp()}.

-type pending_request_pid()   :: pid().
-type pending_request_baton() :: {{pending_request_pid(), reference()}, erlang:timestamp()}.

%% Connection count errors reported by ets_buffer, new buffering strategies may need new error types.
-type connection_count_error() :: ets_buffer:buffer_error().
-type max_connections()        :: pos_integer() | connection_count_error().
-type pending_count()          :: pos_integer() | connection_count_error().

-type idle_status()     :: {idle_connections, {connection_queue_name(),
                                               Idle::max_connections(), Max::max_connections()}}.
-type pending_status()  :: {pending_requests, {requests_queue_name(),
                                               pending_count(), timeout_in_ms()}}.
-type status_reply()    :: {status, {idle_status(), pending_status()}}.

-type pending_checkin() :: {boolean() | pending, {connection_queue_name(),
                                                  max_connections(), max_connections()}}.

%% Errors when a pending request does not get a chance to return a query result.
-type wait_for_session_error() :: {wait_for_session_timeout, pos_integer()}
                                | {wait_for_session_error,   any()}.

%% Errors when a query request fails after submitting to Cassandra.
-type worker_reply_error()     :: {worker_reply_timeout,     pos_integer()}
                                | {worker_reply_error,       any()}.

%% Errors getting a connection to Cassandra.
-type connection_error()       :: {error, no_db_connections}.

%% All errors that a pending request may encounter.
-type pend_request_error()     :: ets_buffer:buffer_error()
                                | wait_for_session_error()
                                | worker_reply_error()
                                | connection_error().

%% Query_reply is a successful return; query_result contains all possible query returns.
-type query_reply()  :: any().
-type query_result() :: pend_request_error() | query_reply().
