%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Buffering strategy is a behaviour for different approaches to buffering
%%%   requests in excess of the number of Cassandra connections.

%%%   The current release provides the following buffering strategy
%%%   implementations:
%%%
%%%     elysium_bs_serial:
%%%        A connection queue and a pending request queue, each
%%%        implemented as a single gen_server with an erlang queue
%%%        as the internal state. Loss of a gen_server loses all
%%%        queue entries.
%%%
%%%     elysium_bs_parallel:
%%%        A connection queue and a pending request queue, each
%%%        implemented as a FIFO ets_buffer. The ets_buffers
%%%        survive all failures up to the elysium_sup itself.
%%%
%%%        As of Nov 13, 2014 this approach has concurrency
%%%        issues related to ets_missing_data errors.
%%%
%%% @since 0.1.7
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_buffering_strategy).
-author('jay@duomark.com').

-include("elysium_types.hrl").
-include("elysium_audit_types.hrl").

%% External API supplied for behaviour implementations to share.
-export([
         status/1,
         create_connection/4,
         decay_causes_death/2,
         decay_connection/3,
         report_available_resources/3
        ]).


%%%-----------------------------------------------------------------------
%%% Callbacks that behaviour implementers must provide
%%%-----------------------------------------------------------------------


%% Checkin connection makes a connection available for use.
-callback checkin_connection(config_type(), cassandra_node(), connection_id(), Is_New_Connection::boolean())
                    -> {boolean() | pending, {connection_queue_name(), Idle_Count, Max_Count}}
                           when Idle_Count :: max_connections(),
                                Max_Count  :: max_connections().

%% Checkout connection dedicates an available connecion to a query request.
-callback checkout_connection(config_type()) -> {cassandra_node(), connection_id()} | none_available.

%% Pending requests wait until a connection is available.
-callback pend_request(config_type(), query_request()) -> query_result().

%% When a connection is available, a pending request can be executed.
-callback handle_pending_request(config_type(), Elapsed_Time::timeout_in_ms(), Reply_Timeout::timeout_in_ms(),
                                 cassandra_node(), connection_id(), query_request()) -> query_result().

%% Create the auditing ets table entry for telemetry counts not specific to a connection.
-callback insert_audit_counts(audit_ets_name()) -> true.

%% Get the current number of idle connections.
-callback idle_connection_count(config_type()) -> max_connections().

%% Get the current number of pending requests.
-callback pending_request_count(config_type()) -> pending_count().

%% Report idle connection count and pending requests count.
-callback status(config_type()) -> status_reply().
                                            
    
%%%-----------------------------------------------------------------------
%%% Buffering strategy provided functions
%%%-----------------------------------------------------------------------

-spec create_connection(config_type(), buffering_strategy_module(), cassandra_node(), connection_id())
                       -> {boolean() | pending, {connection_queue_name(), Idle_Count, Max_Count}}
                              when Idle_Count :: max_connections(),
                                   Max_Count  :: max_connections().

-spec decay_causes_death (config_type(), connection_id()) -> boolean().
-spec decay_connection   (config_type(), buffering_strategy_module(), connection_id()) -> true.

-spec status(config_type()) -> status_reply().
-spec report_available_resources(Queue, Connections, Max_Or_Timeout) -> {Queue, Connections, Max_Or_Timeout}
                                       when Queue          :: queue_name(),
                                            Connections    :: max_connections() | pending_count(),
                                            Max_Or_Timeout :: max_connections() | timeout_in_ms().

%% Do any initialization before checkin of a newly created connection.
create_connection(Config, BS_Module, {_Ip, _Port} = Node, Connection_Id)
  when is_pid(Connection_Id) ->
    elysium_buffering_audit:audit_count_init(Config, BS_Module, Connection_Id),
    BS_Module:checkin_connection(Config, Node, Connection_Id, true).

decay_causes_death(Config, _Connection_Id) ->
    case elysium_config:decay_probability(Config) of
        Never_Decays when is_integer(Never_Decays), Never_Decays =:= 0 ->
            false;
        Probability  when is_integer(Probability),  Probability   >  0, Probability =< 1000000000 ->
            R = elysium_random:random_int_up_to(1000000000),
            R =< Probability
    end.

decay_connection(Config, BS_Module, Connection_Id) ->
    Supervisor_Pid = elysium_queue:get_connection_supervisor(),
    _ = case elysium_connection_sup:stop_child(Supervisor_Pid, Connection_Id) of
            {error, not_found} -> dont_replace_child;
            ok -> elysium_connection_sup:start_child(Supervisor_Pid, [Config])
        end,
    _ = BS_Module:audit_count(Config, session_decay),
    elysium_buffering_audit:audit_data_delete(Config, BS_Module, Connection_Id).

status(Config) ->
    {status, {idle_connections(Config),
              pending_requests(Config)}}.

report_available_resources(Queue_Name, {missing_ets_buffer, Queue_Name}, Max) ->
    {Queue_Name, {missing_ets_buffer, 0, Max}};
report_available_resources(Queue_Name, Num_Sessions, Max) ->
    {Queue_Name, Num_Sessions, Max}.


%%%-----------------------------------------------------------------------
%%% Internal support functions
%%%-----------------------------------------------------------------------

idle_connections(Config) ->
    Connection_Queue = elysium_config:session_queue_name             (Config),
    Max_Connections  = elysium_config:session_max_count              (Config),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module (Config),
    Idle_Count       = BS_Module:idle_connection_count               (Config),
    {idle_connections, report_available_resources(Connection_Queue, Idle_Count, Max_Connections)}.
    
pending_requests(Config) ->
    Pending_Queue    = elysium_config:requests_queue_name            (Config),
    Reply_Timeout    = elysium_config:request_reply_timeout          (Config),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module (Config),
    Pending_Count    = BS_Module:pending_request_count               (Config),
    {pending_requests, report_available_resources(Pending_Queue, Pending_Count, Reply_Timeout)}.
