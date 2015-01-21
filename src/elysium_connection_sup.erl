%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_connection_sup supervises all Cassandra connections.
%%%   Each connection is an instance of a seestar_session (gen_server)
%%%   which is used to execute requests against the Cassandra database.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_connection_sup).
-author('jay@duomark.com').

-behavior(supervisor).

%% External API
-export([
         start_link/0,
         start_child/2,
         stop_child/2
        ]).

%% Internal exports
-export([init/1]).

-include("elysium_types.hrl").


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
%% @doc
%%   Start the connection supervisor which manages replacement of
%%   connections when they die. Connections are maintained as
%%   simple_one_for_one workers without any initial args. When a
%%   Cassandra session is started or restarted, configuration data
%%   is used to determine which node to connect to. This allows a
%%   decayed or failed session to move to another Cassandra node.
%%   The failure may have been caused by the node, and when we
%%   try to get a new connection the round-robin load balancing
%%   will direct us to the next available Cassandra node.
%%
%%   This supervisor is controlled from the elysium_queue FSM.
%%   After initializing, it notifies the FSM that it is ready
%%   so the FSM can begin a controlled startup. When the system
%%   is ready for Cassandra connections, the FSM will call this
%%   supervisor to start_child/2 for each live session wanted.
%% @end
start_link() ->
    {ok, Sup_Pid} = supervisor:start_link(?MODULE, {}),
    ok = elysium_queue:register_connection_supervisor(Sup_Pid),
    _Num_Sessions = elysium_queue:activate(),
    {ok, Sup_Pid}.

-spec start_child(pid(), [config_type()]) -> {ok, pid()}.
%% @doc Start a new Cassandra connection using the config load balanced node list.
start_child(Sup_Pid, [Config | _More] = Args) ->
    true = elysium_config:is_valid_config(Config),
    supervisor:start_child(Sup_Pid, Args).

-spec stop_child(pid(), pid()) -> ok | {error, not_found}.
%% @doc Stop a Cassandra connection.
stop_child(Sup_Pid, Child_Pid) ->
    supervisor:terminate_child(Sup_Pid, Child_Pid).


%%%-----------------------------------------------------------------------
%%% Internal API
%%%-----------------------------------------------------------------------

-define(CHILD(__Mod, __Args),
        {__Mod, {__Mod, start_link, __Args}, permanent, 2000, worker, [__Mod]}).

-spec init({}) -> ignore
                      | {ok, {{simple_one_for_one, pos_integer(), pos_integer()},
                              [supervisor:child_spec()]}}.
%% @doc
%%   Creates a simple_one_for_one elysium_connection child definition
%%   to be used to dynamically add children to this supervisor. All
%%   children register themselves with the elysium queue so they may
%%   be checked out to execute Cassandra queries.
%% @end
init({}) ->
    Child = ?CHILD(elysium_connection, []),
    {ok, {{simple_one_for_one, 1000, 1}, [Child]}}.
