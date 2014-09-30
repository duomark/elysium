%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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
         all_config_atoms/0,
         all_config_bins/0,
         is_valid_config/1,
         is_valid_config_module/1,
         is_valid_config_vbisect/1,
         make_vbisect_config/5,

         session_queue_name/1,
         round_robin_hosts/1,
         session_max_count/1,
         checkout_max_retry/1,
         decay_probability/1
        ]).

-include("elysium_types.hrl").

-callback cassandra_session_queue()             -> queue_name().
-callback cassandra_hosts()                     -> host_list().
-callback cassandra_max_sessions()              -> max_sessions().
-callback cassandra_max_checkout_retry()        -> max_retries().
-callback cassandra_session_decay_probability() -> decay_prob().


%% Configuration parameters may be either compiled functions or keys in a dictionary.
%% These functions are used for validating once before using.

-spec all_config_atoms() -> [atom()].
-spec all_config_bins()  -> [binary()].
-spec is_valid_config(config_type())  -> boolean().

all_config_atoms() ->
    ordsets:from_list([
                       cassandra_hosts,
                       cassandra_session_queue,
                       cassandra_max_sessions,
                       cassandra_max_checkout_retry,
                       cassandra_session_decay_probability
                      ]).

all_config_bins() ->
    [atom_to_binary(Attr, utf8) || Attr <- all_config_atoms()].

is_valid_config({config_mod, Module}) -> is_valid_config_module(Module);
is_valid_config({vbisect,   Bindict}) -> is_valid_config_vbisect(Bindict).
    
is_valid_config_module(Module) when is_atom(Module) ->
    lists:all(fun(Param) -> erlang:function_exported(Module, Param, 0) end, all_config_atoms()).

is_valid_config_vbisect(Bindict) when is_binary(Bindict) ->
    ordsets:is_subset(all_config_bins(), vbisect:fetch_keys(Bindict)).


-spec make_vbisect_config(queue_name(), host_list(), max_sessions(), max_retries(), decay_prob())
                         -> {vbisect, vbisect:bindict()}.

make_vbisect_config(Queue_Name, [{_Ip, _Port} | _] = Host_List, Max_Sessions, Max_Retries, Decay_Prob)
 when is_atom(Queue_Name), is_list(_Ip), is_integer(_Port), _Port > 0,
      is_integer(Max_Sessions), Max_Sessions >  0,
      is_integer(Max_Retries),  Max_Retries  >= 0,
      is_integer(Decay_Prob),   Decay_Prob   >= 0, Decay_Prob =< 1000000 ->

    Props = [
             {<<"cassandra_session_queue">>,              atom_to_binary(Queue_Name, utf8)},
             {<<"cassandra_hosts">>,                      term_to_binary(Host_List)},
             {<<"cassandra_max_sessions">>,               integer_to_binary(Max_Sessions)},
             {<<"cassandra_max_checkout_retry">>,         integer_to_binary(Max_Retries)},
             {<<"cassandra_session_decay_probability">>,  integer_to_binary(Decay_Prob)}
            ],
    {vbisect, vbisect:from_list(Props)}.


%% Configuration accessors are expected to be used frequently.
%% They should have no concurrency contention and are expected
%% to execute as quickly as possible, therefore the raw accessors
%% do not check for the validity of the parameters. These functions
%% will crash if they are passed an invalid parameter.

-spec session_queue_name (config_type()) -> queue_name().
-spec round_robin_hosts  (config_type()) -> host_list().
-spec session_max_count  (config_type()) -> max_sessions().
-spec checkout_max_retry (config_type()) -> max_retries().
-spec decay_probability  (config_type()) -> decay_prob().

session_queue_name ({config_mod,  Config_Module}) -> Config_Module:cassandra_session_queue();
session_queue_name ({vbisect,           Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_session_queue">>, Bindict),
                                                 binary_to_atom(Bin_Value, utf8).

round_robin_hosts  ({config_mod,  Config_Module}) -> Config_Module:cassandra_hosts();
round_robin_hosts  ({vbisect,           Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_hosts">>, Bindict),
                                                 binary_to_term(Bin_Value).

session_max_count  ({config_mod,  Config_Module}) -> Config_Module:cassandra_max_sessions();
session_max_count  ({vbisect,           Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_max_sessions">>, Bindict),
                                                 binary_to_integer(Bin_Value).

checkout_max_retry ({config_mod,  Config_Module}) -> Config_Module:cassandra_max_checkout_retry();
checkout_max_retry ({vbisect,           Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_max_checkout_retry">>, Bindict),
                                                 binary_to_integer(Bin_Value).

decay_probability  ({config_mod,  Config_Module}) -> Config_Module:cassandra_session_decay_probability();
decay_probability  ({vbisect,           Bindict}) -> {ok, Bin_Value} = vbisect:find(<<"cassandra_session_decay_probability">>, Bindict),
                                                 binary_to_integer(Bin_Value).

