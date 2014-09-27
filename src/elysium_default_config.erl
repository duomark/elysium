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
         cassandra_session_queue/0,
         cassandra_hosts/0,
         cassandra_max_sessions/0,
         cassandra_max_checkout_retry/0,
         cassandra_session_decay_probability/0
        ]).

-behaviour(elysium_config).
-include("elysium_types.hrl").

-spec cassandra_session_queue() -> ets_buffer:buffer_name().
%% @doc The name of the ets_buffer for the active session queue.
cassandra_session_queue() -> elysium_connection_queue.

-spec cassandra_hosts() -> host_list().
%% @doc The list of Cassandra hosts and ports to connect sessions.
cassandra_hosts() -> [{"127.0.0.1", 9042}].

-spec cassandra_max_sessions() -> pos_integer().
%% @doc The maximum number of live Cassandra sessions simultaneously active.
cassandra_max_sessions() -> 16.

-spec cassandra_max_checkout_retry() -> pos_integer().
%% @doc The maximum number of times to retry on checkout if transient errors.
cassandra_max_checkout_retry() -> 20.

-spec cassandra_session_decay_probability() -> pos_integer().
%% @doc Number of chances that a live session should be killed, per 1M uses.
cassandra_session_decay_probability() -> 1000.
    
