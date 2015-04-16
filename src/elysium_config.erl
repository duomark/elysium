%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Because elysium is used in high-concurrency scenarios to enhance
%%%   performance, it needs to avoid process contention. One bottleneck
%%%   is accessing ets tables because they have a limited number of locks
%%%   available (even though they are the most concurrent of the erlang data
%%%   structures that can be modified), and are especially susceptible
%%%   to contention when multiple processes access the same key since
%%%   there is one lock per key which they must queue behind.
%%%
%%%   The problem with application configuration data is that it is
%%%   stored in an ets table by the application_controller. Worse,
%%%   all processes that access a given configuration parameter will
%%%   be accessing the same key. Now imagine a dynamically configurable
%%%   host/port used for Cassandra sessions, or the need to dynamically
%%%   change the session decay characteristics or even the queue_name
%%%   (since you may want to have more than one elysium running which
%%%   connects to more than one Cassandra cluster for different data).
%%%
%%%   To allow the most flexibility, two alternatives to the standard
%%%   application configuration are offered:
%%%
%%%     1) Compiled configuration code
%%%     2) Binary encoded dictionary
%%%
%%%   Compiled code is globally available without locking and it can
%%%   be modified with a hot-code load or the generation and compilation
%%%   on-the-fly of a replacement module. It is the fastest method for
%%%   provided direct concurrent access to data.
%%%
%%%   A binary encoded dictionary which is greater than 64 bytes will
%%%   be stored on the binary heap. This provides concurrent read-only
%%%   access to all processes. The vbisect library used here runs at
%%%   the speed of an ets table with a single process accessing it when
%%%   the binary contains approximately 10 attributes. The difference
%%%   is that the ets performance will suffer with concurrent access
%%%   whereas the vbisect will not slow down regardless of the number
%%%   of concurrent processes accessing it. The difficulty lies in
%%%   the application writer passing the instance of the dictionary
%%%   around without stashing it in a gen_server or some other
%%%   stateful serializing structure.
%%%
%%%   Just beware that maintaining more than one configuration and
%%%   dynamically swapping them between calls can lead to errors
%%%   whereby the active session queue might not be available. In
%%%   general, all calls work as expected regardless if the same
%%%   configuration is used in every call, but it is up to the
%%%   application writer to avoid creating conflicts when juggling
%%%   multiple configurations. A typical simple application will
%%%   set up one configuration at start up time and will run with
%%%   that single configuration continuously.
%%%
%%% @since 0.1.2
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_config).
-author('jay@duomark.com').

-export([
         all_config_atoms_ordered/0,
         all_config_atoms_sorted/0,
         all_config_bins/0,
         is_valid_config/1,
         is_valid_config_module/1,
         is_valid_config_vbisect/1,
         make_vbisect_config/17,
         make_vbisect_config/1,

         is_elysium_config_enabled/1,
         load_balancer_queue/1,
         connection_buffering_strategy/1,
         audit_ets_name/1,
         session_queue_name/1,
         requests_queue_name/1,
         request_reply_timeout/1,
         round_robin_hosts/1,
         max_restart_delay/1,
         connect_timeout/1,
         send_timeout/1,
         session_max_count/1,
         checkout_max_retry/1,
         decay_probability/1,
         seed_node/1,
         request_peers_frequency/1,
         default_port/1
        ]).

-include("elysium_types.hrl").

-callback is_elysium_enabled()                  -> boolean().
-callback cassandra_lb_queue()                  -> lb_queue_name().
-callback cassandra_connection_bs()             -> elysium_connection:buffering().
-callback cassandra_audit_ets()                 -> audit_ets_name().
-callback cassandra_session_queue()             -> connection_queue_name().
-callback cassandra_requests_queue()            -> requests_queue_name().
-callback cassandra_request_reply_timeout()     -> timeout_in_ms().
-callback cassandra_hosts()                     -> host_list().
-callback cassandra_max_restart_delay()         -> timeout_in_ms().
-callback cassandra_connect_timeout()           -> timeout_in_ms().
-callback cassandra_send_timeout()              -> timeout_in_ms().
-callback cassandra_max_sessions()              -> max_connections().
-callback cassandra_max_checkout_retry()        -> max_retries().
-callback cassandra_session_decay_probability() -> decay_prob().


%% Configuration parameters may be either compiled functions or keys in a dictionary.
%% These functions are used for validating once before using.

-spec all_config_atoms_ordered() -> [atom()].
%% @doc Get a list of all the configuration parameter keys as atoms in the order needed by make_vbisect_config/17.
all_config_atoms_ordered() ->
    [
     is_elysium_enabled,
     cassandra_lb_queue,
     cassandra_connection_bs,
     cassandra_audit_ets,
     cassandra_session_queue,
     cassandra_requests_queue,
     cassandra_request_reply_timeout,
     cassandra_hosts,
     cassandra_connect_timeout,
     cassandra_send_timeout,
     cassandra_max_restart_delay,
     cassandra_max_sessions,
     cassandra_max_checkout_retry,
     cassandra_session_decay_probability,
     cassandra_seed_node,
     cassandra_request_peers_frequency,
     cassandra_default_port
    ].

-spec all_config_atoms_sorted()  -> [atom()].
%% @doc Get a list of all the configuration parameter keys as atoms sorted alphabetically.
all_config_atoms_sorted() ->
    ordsets:from_list(all_config_atoms_ordered()).

-spec all_config_bins()  -> [binary()].
%% @doc Get a list of all the configuration parameter keys as binaries.
all_config_bins() ->
    [atom_to_binary(Attr, utf8) || Attr <- all_config_atoms_ordered()].

-spec is_valid_config(config_type())  -> boolean().
%% @doc Verify that the configuration is a valid construct and has all the parameter attributes supported.
is_valid_config ({config_app_config, App}) -> true;
is_valid_config ({vbisect,       Bindict}) -> is_valid_config_vbisect(Bindict);
is_valid_config ({config_mod,     Module}) -> {module, Module} = code:ensure_loaded(Module),
                                              is_valid_config_module(Module).
    
-spec is_valid_config_module(module())  -> boolean().
%% @doc Verify that a compiled module configuration has a function for every configuration parameter.
is_valid_config_module(Module) when is_atom(Module) ->
    lists:all(fun(Param) -> erlang:function_exported(Module, Param, 0) end, all_config_atoms_ordered()).

-spec is_valid_config_vbisect(vbisect:bindict())  -> boolean().
%% @doc Verify that a binary dictionary contains all the keys corresponding to configuration parameters.
is_valid_config_vbisect(Bindict) when is_binary(Bindict) ->
    ordsets:is_subset(
      ordsets:from_list(all_config_bins()),
      ordsets:from_list(vbisect:fetch_keys(Bindict))
     ).


-spec make_vbisect_config(boolean(), lb_queue_name(), elysium_connection:buffering(),
                          audit_ets_name(), connection_queue_name(), requests_queue_name(),
                          timeout_in_ms(), host_list(), timeout_in_ms(), timeout_in_ms(), timeout_in_ms(),
                          max_connections(), max_retries(), decay_prob(),
                          cassandra_node(), request_peers_frequency(),
                          inet:port_number()) -> {vbisect, vbisect:bindict()}.
%% @doc
%%   Construct a vbisect binary dictionary from individual configuration parameter values.
%%   The resulting data structure may be passed as a configuration to any of the elysium functions.
%% @end
make_vbisect_config(Enabled, Lb_Queue_Name, Buffering_Strategy,
                    Audit_Ets_Name, Connection_Queue_Name, Requests_Queue_Name,
                    Request_Reply_Timeout, [{_Ip, _Port} | _] = Host_List,
                    Connect_Timeout_Millis, Send_Timeout_Millis, Restart_Millis,
                    Max_Connections, Max_Retries, Decay_Prob, {Seed_Host, Seed_Port} = Seed_Node,
                    Request_Peers_Frequency_Millis, Default_Port)
 when is_atom(Buffering_Strategy),
      is_atom(Lb_Queue_Name),             is_atom(Audit_Ets_Name),
      is_atom(Connection_Queue_Name),     is_atom(Requests_Queue_Name),
      is_list(_Ip), is_integer(_Port),    _Port > 0,
      is_integer(Request_Reply_Timeout),  Request_Reply_Timeout  > 0,
      is_integer(Connect_Timeout_Millis), Connect_Timeout_Millis > 0,
      is_integer(Send_Timeout_Millis),    Send_Timeout_Millis    > 0,
      is_integer(Restart_Millis),         Restart_Millis         > 0,
      is_integer(Max_Connections),        Max_Connections        > 0,
      is_integer(Max_Retries),            Max_Retries >= 0,
      is_integer(Decay_Prob),             Decay_Prob  >= 0, Decay_Prob =< 1000000000,
      is_integer(Seed_Port),              Seed_Port   >= 0,
      is_list   (Seed_Host),
      is_integer(Request_Peers_Frequency_Millis), Request_Peers_Frequency_Millis > 0,
      is_integer(Default_Port), Default_Port > 0, Default_Port =< 65535 ->

    Props = [
             {<<"is_elysium_enabled">>,                   boolean_to_binary (Enabled)},
             {<<"cassandra_lb_queue">>,                   atom_to_binary    (Lb_Queue_Name,         utf8)},
             {<<"cassandra_connection_bs">>,              atom_to_binary    (Buffering_Strategy,    utf8)},
             {<<"cassandra_audit_ets">>,                  atom_to_binary    (Audit_Ets_Name,        utf8)},
             {<<"cassandra_session_queue">>,              atom_to_binary    (Connection_Queue_Name, utf8)},
             {<<"cassandra_requests_queue">>,             atom_to_binary    (Requests_Queue_Name,   utf8)},
             {<<"cassandra_request_reply_timeout">>,      integer_to_binary (Request_Reply_Timeout)},
             {<<"cassandra_hosts">>,                      term_to_binary    (Host_List)},
             {<<"cassandra_max_sessions">>,               integer_to_binary (Max_Connections)},
             {<<"cassandra_max_restart_delay">>,          integer_to_binary (Restart_Millis)},
             {<<"cassandra_connect_timeout">>,            integer_to_binary (Connect_Timeout_Millis)},
             {<<"cassandra_send_timeout">>,               integer_to_binary (Send_Timeout_Millis)},
             {<<"cassandra_max_checkout_retry">>,         integer_to_binary (Max_Retries)},
             {<<"cassandra_session_decay_probability">>,  integer_to_binary (Decay_Prob)},
             {<<"cassandra_seed_node">>,                  term_to_binary    (Seed_Node)},
             {<<"cassandra_request_peers_frequency">>,    integer_to_binary (Request_Peers_Frequency_Millis)},
             {<<"cassandra_default_port">>,               integer_to_binary (Default_Port)}
            ],
    {vbisect, vbisect:from_list(Props)}.

-spec make_vbisect_config(proplists:proplist()) -> {vbisect, vbisect:bindict()}.
%% @doc
%%   Construct a vbisect binary dictionary from an entire set of app.config configuration parameters.
%%   The resulting data structure may be passed as a configuration to any of the elysium functions.
%% @end
make_vbisect_config(Config_Dictionary) ->
    Attrs       = elysium_config:all_config_atoms_ordered(),
    Attr_Values = [proplists:get_value(Attr, Config_Dictionary) || Attr <- Attrs],
    apply(elysium_config, make_vbisect_config, Attr_Values).


%% Configuration accessors are expected to be used frequently.
%% They should have no concurrency contention and must execute
%% as quickly as possible, therefore the raw accessors do not
%% check for the validity of the parameters. These functions
%% will crash if they are passed an invalid parameter.

get_app_config(App, Param) -> {ok, Value} = application:get_env(App, Param), Value.
     
-spec is_elysium_config_enabled (config_type()) -> boolean().
%% @doc Determine if elysium is enabled.
is_elysium_config_enabled ({config_app_config,    App}) -> get_app_config(App, is_elysium_enabled);
is_elysium_config_enabled ({config_mod, Config_Module}) -> Config_Module:is_elysium_enabled();
is_elysium_config_enabled ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"is_elysium_enabled">>, Bindict),
                                                            binary_to_boolean(Bin_Value).

-spec load_balancer_queue (config_type()) -> lb_queue_name().
%% @doc Get the configured name of the round-robin load balancer queue.
load_balancer_queue ({config_app_config,    App}) -> get_app_config(App, cassandra_lb_queue);
load_balancer_queue ({config_mod, Config_Module}) -> Config_Module:cassandra_lb_queue();
load_balancer_queue ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_lb_queue">>, Bindict),
                                                      binary_to_atom(Bin_Value, utf8).

-spec connection_buffering_strategy (config_type()) -> elysium_connection:buffering().
%% @doc Get the configured name of the buffering strategy.
connection_buffering_strategy ({config_app_config,    App}) -> get_app_config(App, cassandra_connection_bs);
connection_buffering_strategy ({config_mod, Config_Module}) -> Config_Module:cassandra_connection_bs();
connection_buffering_strategy ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_connection_bs">>, Bindict),
                                                                 binary_to_atom(Bin_Value, utf8).

-spec audit_ets_name (config_type()) -> audit_ets_name().
%% @doc Get the configured name of the ets table used for auditing internals.
audit_ets_name ({config_app_config,    App}) -> get_app_config(App, cassandra_audit_ets);
audit_ets_name ({config_mod, Config_Module}) -> Config_Module:cassandra_audit_ets();
audit_ets_name ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_audit_ets">>, Bindict),
                                                      binary_to_atom(Bin_Value, utf8).

-spec session_queue_name (config_type()) -> connection_queue_name().
%% @doc Get the configured name of the live session queue.
session_queue_name ({config_app_config,    App}) -> get_app_config(App, cassandra_session_queue);
session_queue_name ({config_mod, Config_Module}) -> Config_Module:cassandra_session_queue();
session_queue_name ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_session_queue">>, Bindict),
                                                      binary_to_atom(Bin_Value, utf8).

-spec requests_queue_name (config_type()) -> requests_queue_name().
%% @doc Get the configured name of the pending requests queue.
requests_queue_name ({config_app_config,    App}) -> get_app_config(App, cassandra_requests_queue);
requests_queue_name ({config_mod, Config_Module}) -> Config_Module:cassandra_requests_queue();
requests_queue_name ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_requests_queue">>, Bindict),
                                                      binary_to_atom(Bin_Value, utf8).

-spec request_reply_timeout  (config_type()) -> timeout_in_ms().
%% @doc Get the time allowed for a pending query request to wait for a session before giving up.
request_reply_timeout ({config_app_config,   App}) -> get_app_config(App, cassandra_request_reply_timeout);
request_reply_timeout ({config_mod, Config_Module}) -> Config_Module:cassandra_request_reply_timeout();
request_reply_timeout ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_request_reply_timeout">>, Bindict),
                                                     binary_to_integer(Bin_Value).

-spec round_robin_hosts  (config_type()) -> host_list().
%% @doc Get the configured set of cassandra nodes to contact.
round_robin_hosts ({config_app_config,    App}) -> get_app_config(App, cassandra_hosts);
round_robin_hosts ({config_mod, Config_Module}) -> Config_Module:cassandra_hosts();
round_robin_hosts ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_hosts">>, Bindict),
                                                     binary_to_term(Bin_Value).

-spec max_restart_delay  (config_type()) -> timeout_in_ms().
%% @doc
%%   Get the maximum random delay on connection startup. This number of
%%   milliseconds times the max_connections should not exceed the supervisor
%%   timeout or you risk failing elysium_connection_sup on application
%%   startup.
%% @end
max_restart_delay ({config_app_config,    App}) -> get_app_config(App, cassandra_max_restart_delay);
max_restart_delay ({config_mod, Config_Module}) -> Config_Module:cassandra_max_restart_delay();
max_restart_delay ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_max_restart_delay">>, Bindict),
                                                     binary_to_integer(Bin_Value).

-spec connect_timeout  (config_type()) -> timeout_in_ms().
%% @doc Get the time allowed for a cassandra connection before giving up.
connect_timeout ({config_app_config,    App}) -> get_app_config(App, cassandra_connect_timeout);
connect_timeout ({config_mod, Config_Module}) -> Config_Module:cassandra_connect_timeout();
connect_timeout ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_connect_timeout">>, Bindict),
                                                     binary_to_integer(Bin_Value).

-spec send_timeout      (config_type()) -> timeout_in_ms().
%% @doc Get the time allowed for sending a request to cassandra before giving up.
send_timeout ({config_app_config,    App}) -> get_app_config(App, cassandra_send_timeout);
send_timeout ({config_mod, Config_Module}) -> Config_Module:cassandra_send_timeout();
send_timeout ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_send_timeout">>, Bindict),
                                                     binary_to_integer(Bin_Value).

-spec session_max_count  (config_type()) -> max_connections().
%% @doc Get the maximum number of live sessions that can be open simultaneously.
session_max_count ({config_app_config,    App}) -> get_app_config(App, cassandra_max_sessions);
session_max_count ({config_mod, Config_Module}) -> Config_Module:cassandra_max_sessions();
session_max_count ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_max_sessions">>, Bindict),
                                                     binary_to_integer(Bin_Value).

-spec checkout_max_retry (config_type()) -> max_retries().
%% @doc Get the number of retries on transient failure when retrieving a live session from the queue.
checkout_max_retry ({config_app_config,    App}) -> get_app_config(App, cassandra_max_checkout_retry);
checkout_max_retry ({config_mod, Config_Module}) -> Config_Module:cassandra_max_checkout_retry();
checkout_max_retry ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_max_checkout_retry">>, Bindict),
                                                     binary_to_integer(Bin_Value).

-spec decay_probability  (config_type()) -> decay_prob().
%% @doc Get the number of chances in 1 Million that this session will be stochastically recycled before checkin.
decay_probability ({config_app_config,    App}) -> get_app_config(App, cassandra_session_decay_probability);
decay_probability ({config_mod, Config_Module}) -> Config_Module:cassandra_session_decay_probability();
decay_probability ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_session_decay_probability">>, Bindict),
                                                     binary_to_integer(Bin_Value).

-spec seed_node          (config_type()) -> cassandra_node().
%% @doc Get the seed seestar node from where to retrieve the list of peers.
seed_node ({config_app_config,    App})  -> get_app_config(App, cassandra_seed_node);
seed_node ({config_mod, Config_Module}) -> Config_Module:cassandra_seed_node();
seed_node ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_seed_node">>, Bindict),
                                           binary_to_term(Bin_Value).

-spec request_peers_frequency(config_type()) -> request_peers_frequency().
%% @doc Get the time between consecutive seestar peers requests.
request_peers_frequency ({config_app_config,    App}) -> get_app_config(App, cassandra_request_peers_frequency);
request_peers_frequency ({config_mod, Config_Module}) -> Config_Module:cassandra_request_peers_frequency();
request_peers_frequency ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_request_peers_frequency">>, Bindict),
                                                         binary_to_integer(Bin_Value).

-spec default_port(config_type()) -> inet:port_number().
%% @doc Get the default port for elysium requests
default_port ({config_app_config,    App}) -> get_app_config(App, cassandra_default_port);
default_port ({config_mod, Config_Module}) -> Config_Module:cassandra_default_port();
default_port ({vbisect,          Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_default_port">>, Bindict),
                                              binary_to_integer(Bin_Value).

boolean_to_binary(true)    -> <<"1">>;
boolean_to_binary(false)   -> <<"0">>.
    
binary_to_boolean(<<"1">>) -> true;
binary_to_boolean(<<"0">>) -> false.
