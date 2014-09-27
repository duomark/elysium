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
%%%   The current coding style in elysium assumes that any of the
%%%   configuration data can change at anytime (for example, to round
%%%   robin between different connection queues), so the configuration
%%%   data needs to be globally concurrent as much as possible. Once
%%%   you realize that compiled code is globally available concurrently,
%%%   it becomes obvious (or an obviously dirty hack) that the contended
%%%   configuration data should be kept in code rather than data and
%%%   should be modified by compiling a replacement module or by doing
%%%   a hot-code load of the new parameters as a changed module.
%%%
%%%   Just beware that maintaining more than one configuration module
%%%   and dynamically swapping them between calls can lead to errors
%%%   whereby the active session queue might not be available. In
%%%   general, all calls work as expected regardless if the same
%%%   configuration module is used in every call.
%%%
%%% @since 0.1.2
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_config).
-author('jay@duomark.com').

-export([
         worker_queue_name/1,
         round_robin_hosts/1,
         worker_max_count/1,
         checkout_max_retry/1,
         decay_probability/1
        ]).

-include("elysium_types.hrl").

-callback cassandra_session_queue()             -> ets_buffer:buffer_name().
-callback cassandra_hosts()                     -> host_list().
-callback cassandra_max_sessions()              -> pos_integer().
-callback cassandra_max_checkout_retry()        -> pos_integer().
-callback cassandra_session_decay_probability() -> pos_integer().

-spec worker_queue_name  (module()) -> ets_buffer:buffer_name().
-spec round_robin_hosts  (module()) -> host_list().
-spec worker_max_count   (module()) -> pos_integer().
-spec checkout_max_retry (module()) -> non_neg_integer().
-spec decay_probability  (module()) -> non_neg_integer().

worker_queue_name(Config_Module) ->
    Config_Module:cassandra_session_queue().

round_robin_hosts(Config_Module) ->
    Config_Module:cassandra_hosts().

worker_max_count(Config_Module) ->
    Config_Module:cassandra_max_sessions().

checkout_max_retry(Config_Module) ->
    Config_Module:cassandra_max_checkout_retry().

decay_probability(Config_Module) ->
    Config_Module:cassandra_session_decay_probability().
