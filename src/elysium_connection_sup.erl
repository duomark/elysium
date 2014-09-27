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
-export([start_link/3, init/1]).

-include("elysium_types.hrl").


%%%-----------------------------------------------------------------------
%%% External API
%%%-----------------------------------------------------------------------
-spec start_link(module(), host_list(), pos_integer()) -> {ok, pid()}.
%% @doc
%%   Start the connection supervisor for a specific Cassandra Ip + Port.
%%   The number of connections translates to the number of children
%%   started and maintained by the supervisor at all times.
%% @end
start_link(Config_Module, Nodes, Num_Connections)
  when is_atom(Config_Module), is_list(Nodes),
       is_integer(Num_Connections), Num_Connections > 0 ->

    supervisor:start_link({local, ?MODULE}, ?MODULE,
                          {Config_Module, Nodes, Num_Connections}).


%%%-----------------------------------------------------------------------
%%% Internal API
%%%-----------------------------------------------------------------------
-define(CHILD(__Name, __Mod, __Args),
        {__Name, {__Mod, start_link, __Args}, permanent, 2000, worker, [__Mod]}).

-spec init({module(), host_list(), pos_integer()})
          -> {ok, {{supervisor:strategy(), non_neg_integer(), non_neg_integer()},
                   [supervisor:child_spec()]}}.
%% @doc
%%   Creates a separate one_for_one elysium_connection child for each
%%   of the number of simultaneous connections to Cassandra desired.
%% @end
init({Config_Module, Nodes, Num_Connections}) ->
    Names = [list_to_atom("elysium_connection_" ++ integer_to_list(N))
             || N <- lists:seq(1, Num_Connections)],
    Node_Queue = queue:from_list([Node || {_Ip, _Port} = Node <- Nodes]),
    Children   = make_childspecs(Config_Module, Node_Queue, Names, []),
    {ok, {{one_for_one, 1000, 1}, Children}}.

make_childspecs(Config_Module, _Node_Queue,            [], Specs) -> Specs;
make_childspecs(Config_Module,  Node_Queue, [Name | More], Specs) ->
    {{value, {Ip, Port} = Node}, Q} = queue:out(Node_Queue),
    New_Spec = ?CHILD(Name, elysium_connection, [Config_Module, Ip, Port]),
    make_childspecs(Config_Module, queue:in(Node, Q), More, [New_Spec | Specs]).
