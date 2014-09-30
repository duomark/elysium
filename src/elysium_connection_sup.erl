%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_connection_sup supervises all Cassandra connections.
%%%   Each connection is an instance of an elysium_connection (gen_fsm)
%%%   which holds a live seestar_session to a Cassandra database.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_connection_sup).
-author('jay@duomark.com').

-behavior(supervisor).

%% External API
-export([start_link/1, init/1]).

-include("elysium_types.hrl").


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
%% @doc
%%   Start the connection supervisor which manages replacement of
%%   connections when they die. This supervisor has one child for
%%   each Cassandra session allowed simultaneously.
%% @end
start_link(Config) ->
    true = elysium_config:is_valid_config(Config),
    supervisor:start_link({local, ?MODULE}, ?MODULE, {Config}).


%%%-----------------------------------------------------------------------
%%% Internal API
%%%-----------------------------------------------------------------------

-define(CHILD(__Name, __Mod, __Args),
        {__Name, {__Mod, start_link, __Args}, permanent, 2000, worker, [__Mod]}).

-spec init({config_type()})
          -> {ok, {{supervisor:strategy(), non_neg_integer(), non_neg_integer()},
                   [supervisor:child_spec()]}}.
%% @doc
%%   Creates a separate one_for_one elysium_connection child for each
%%   of the number of simultaneous connections to Cassandra desired.
%% @end
init({Config}) ->
    %% Create session names only for the supervisor list, not registered names.
    Num_Sessions = elysium_config:session_max_count(Config),
    Names = [list_to_atom("elysium_connection_" ++ integer_to_list(N))
             || N <- lists:seq(1, Num_Sessions)],

    %% Filter out any malformed Host IP / Port pairs...
    Cassandra_Nodes = elysium_config:round_robin_hosts(Config),
    Node_Queue = queue:from_list([Node || {_Ip, _Port} = Node <- Cassandra_Nodes,
                                          is_list(_Ip), is_integer(_Port), _Port > 0]),
    Children   = make_childspecs(Config, Node_Queue, Names, []),
    {ok, {{one_for_one, 1000, 1}, Children}}.

make_childspecs(_Config, _Node_Queue,            [], Specs) -> Specs;
make_childspecs( Config,  Node_Queue, [Name | More], Specs) ->
    {{value, {Ip, Port} = Node}, Q} = queue:out(Node_Queue),
    New_Spec = ?CHILD(Name, elysium_connection, [Config, Ip, Port]),
    make_childspecs(Config, queue:in(Node, Q), More, [New_Spec | Specs]).
