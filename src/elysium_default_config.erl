%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   This module is an example of standard elysium configuration. You can
%%%   use this one for testing on a local machine, but a real application
%%%   configuration should have remote IPs and Ports for the Cassandra
%%%   cluster on real servers.
%%%
%%% @since 0.1.2
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_default_config).
-author('jay@duomark.com').

-export([
         cassandra_lb_queue/0,
         cassandra_session_queue/0,
         cassandra_hosts/0,
         cassandra_max_sessions/0,
         cassandra_max_checkout_retry/0,
         cassandra_session_decay_probability/0
        ]).

-behaviour(elysium_config).
-include("elysium_types.hrl").

-spec cassandra_lb_queue() -> lb_queue_name().
%% @doc Using load balancer queue 'elysium_lb_queue'.
cassandra_lb_queue() -> elysium_lb_queue.

-spec cassandra_session_queue() -> session_queue_name().
%% @doc Using session queue 'elysium_connection_queue'.
cassandra_session_queue() -> elysium_connection_queue.

-spec cassandra_hosts() -> host_list().
%% @doc Only local host: [{"127.0.0.1", 9042}].
cassandra_hosts() -> [{"127.0.0.1", 9042}].

-spec cassandra_max_sessions() -> max_sessions().
%% @doc Allow a max of 16 simultaneous live Cassandra sessions.
cassandra_max_sessions() -> 5.

-spec cassandra_max_checkout_retry() -> max_retries().
%% @doc Retry up to 20 times when session queue races occur.
cassandra_max_checkout_retry() -> 3.

-spec cassandra_session_decay_probability() -> decay_prob().
%% @doc Kill a live session 1000 out of 1M times it is used.
cassandra_session_decay_probability() -> 1000.
    
