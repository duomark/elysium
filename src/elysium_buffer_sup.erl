%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   The elysium buffer supervisor is an empty supervisor. Its only
%%%   purpose is to own the session and pending request ets_buffer
%%%   queues. These ets tables must have an owner that won't go down.
%%%   The first supervisor in a rest_for_one top level configuration
%%%   means that elysium_buffer_sup will be the first process to start
%%%   and the last to go down. It also avoids placing the ownership
%%%   of these queues with a gen_server or gen_fsm which serves the
%%%   double purpose of responding to queries. Such a setup would
%%%   risk losing the queues if there was a query request which could
%%%   crash the gen_server or gen_fsm. All queries about the queues
%%%   are handled by elysium_queue which is the 2nd process to start
%%%   after elysium_buffer_sup.
%%%
%%%   To further support fault tolerance and load balancing, the configuration
%%%   data identifies a list of Cassandra hosts to which live sessions
%%%   may be established. The list of hosts is kept in a queue so that
%%%   every attempt to connect or reconnect is a round-robin request against
%%%   a different host, skipping any that cannot successfully accept a new
%%%   connection request. Coupled with stochastic decay of existing connections
%%%   the active sessions will automatically adapt to the spontaneous
%%%   availability of Cassandra cluster client nodes for transactions.
%%%
%%% @since 0.1.6
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_buffer_sup).
-author('jay@duomark.com').

-behavior(supervisor).

%% External API
-export([start_link/1, init/1]).

-include("elysium_types.hrl").

-define(SUPER, ?MODULE).


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
%% @doc
%%   Start the root supervisor. This is the one that should be started
%%   by any including supervisor. The config/sys.config provides a set
%%   of example parameters that the including application should specify.
%% @end
start_link(Config) ->
    supervisor:start_link({local, ?SUPER}, ?MODULE, {Config}).


-spec init({config_type()}) -> {ok, {{one_for_one, non_neg_integer(), non_neg_integer()}, []}}.
%% @doc
%%   Starts the gen_fsm which owns the Cassandra connection queue,
%%   and one supervisor of all Cassandra node supervisors. They are
%%   rest_for_one so that the queue is guaranteed to exist before
%%   any connections are created.
%% @end
init({Config}) ->
    %% Setup a load balancing FIFO Queue for all the Cassandra nodes to contact.
    Lb_Queue_Name = elysium_config:load_balancer_queue(Config),
    Lb_Queue_Name = ets_buffer:create_dedicated(Lb_Queue_Name, fifo),
    lager:info("Creating Cassandra round-robin ets_buffer '~p' with the following nodes:~n", [Lb_Queue_Name]),
    _ = [begin
             _ = ets_buffer:write_dedicated(Lb_Queue_Name, Node),
             lager:info("   ~p~n", [Node])
         end || {_Ip, _Port} = Node <- elysium_config:round_robin_hosts(Config),
                is_list(_Ip), is_integer(_Port), _Port > 0],

    %% Create a FIFO Queue for the live sessions that are connected to Cassandra.
    Session_Queue_Name = elysium_config:session_queue_name(Config),
    lager:info("Creating Cassandra session ets_buffer '~p'~n", [Session_Queue_Name]),
    Session_Queue_Name = ets_buffer:create_dedicated(Session_Queue_Name, fifo),

    %% Create a FIFO Queue for pending query requests.
    Pending_Queue_Name = elysium_config:requests_queue_name(Config),
    lager:info("Creating Cassandra pending_requests ets_buffer '~p'~n", [Pending_Queue_Name]),
    Pending_Queue_Name = ets_buffer:create_dedicated(Pending_Queue_Name, fifo),

    %% Create an Audit table for connection checkin/checkout tracking
    Audit_Name = elysium_config:audit_ets_name(Config),
    lager:info("Creating Cassandra audit ets table '~p'~n", [Audit_Name]),
    Audit_Name = ets:new(Audit_Name, [named_table, public, set, {keypos, 2}]),
    {_BS, BS_Module} = elysium_connection:get_buffer_strategy_module(Config),
    elysium_buffering_audit:audit_count_init(Config, BS_Module),

    {ok, {{one_for_one, 1, 10}, []}}.
