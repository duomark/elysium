%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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

-behaviour(elysium_config).

-export([
         is_elysium_enabled/0,
         cassandra_lb_queue/0,
         cassandra_connection_bs/0,
         cassandra_audit_ets/0,
         cassandra_session_queue/0,
         cassandra_requests_queue/0,
         cassandra_request_reply_timeout/0,
         cassandra_hosts/0,
         cassandra_max_restart_delay/0,
         cassandra_connect_timeout/0,
         cassandra_send_timeout/0,
         cassandra_max_sessions/0,
         cassandra_max_checkout_retry/0,
         cassandra_session_decay_probability/0,
         cassandra_seed_node/0,
         cassandra_request_peers_frequency/0,
         cassandra_default_port/0
        ]).

-include("elysium_types.hrl").

-spec is_elysium_enabled() -> boolean().
%% @doc Whether elysium is enabled.
is_elysium_enabled() -> true.
     
-spec cassandra_lb_queue() -> lb_queue_name().
%% @doc Using load balancer queue 'elysium_lb_queue'.
cassandra_lb_queue() -> elysium_lb_queue.

-spec cassandra_connection_bs() -> elysium_connection:buffering().
%% @doc Using parallel ets_buffers for connections and pending requests.
cassandra_connection_bs() -> parallel.

-spec cassandra_audit_ets() -> audit_ets_name().
%% @doc Using ets table 'elysium_audit'.
cassandra_audit_ets() -> elysium_audit.

-spec cassandra_session_queue() -> connection_queue_name().
%% @doc Using connection queue 'elysium_connection_queue'.
cassandra_session_queue() -> elysium_connection_queue.

-spec cassandra_requests_queue() -> requests_queue_name().
%% @doc Using pending requests queue 'elysium_requests_queue'.
cassandra_requests_queue() -> elysium_requests_queue.

-spec cassandra_request_reply_timeout() -> timeout_in_ms().
%% @doc Pending query requests should only wait 5 seconds for an available session.
cassandra_request_reply_timeout() -> 5000.

-spec cassandra_hosts() -> host_list().
%% @doc Only local host: [{"127.0.0.1", 9042}].
cassandra_hosts() -> [{"127.0.0.1", cassandra_default_port()}].

-spec cassandra_max_restart_delay() -> timeout_in_ms().
%% @doc Randomly delay connect from 1-100 milliseconds on startup.
cassandra_max_restart_delay() -> 100.

-spec cassandra_connect_timeout() -> timeout_in_ms().
%% @doc Timeout after 50 milliseconds if a seestar session can't be established.
cassandra_connect_timeout() -> 50.

-spec cassandra_send_timeout() -> timeout_in_ms().
%% @doc Timeout when sending requests to Cassandra.
cassandra_send_timeout() -> 1000.
     
-spec cassandra_max_sessions() -> max_connections().
%% @doc Allow a max of 16 simultaneous live Cassandra sessions.
cassandra_max_sessions() -> 3.

-spec cassandra_max_checkout_retry() -> max_retries().
%% @doc Retry up to 20 times when session queue races occur.
cassandra_max_checkout_retry() -> 5.

-spec cassandra_session_decay_probability() -> decay_prob().
%% @doc Kill a live session 100,000 out of 1 Billion times it is used.
cassandra_session_decay_probability() -> 100000.
    
-spec cassandra_seed_node() -> cassandra_node().
%% @doc 
%%  The node where elysium requests the full c* node list.
%%  There is no default seed node, hence the non-routable address.
%% @end
cassandra_seed_node() -> {"0.0.0.0", 0}.

-spec cassandra_request_peers_frequency() -> request_peers_frequency().
%% @doc Check for seestar nodes once a minute
cassandra_request_peers_frequency() -> 60000.

-spec cassandra_default_port() -> inet:port_number().
%% @doc The default port for elysium requests
cassandra_default_port() -> 9042.
