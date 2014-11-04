%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium contains a gen_fsm which manages an ets_buffer FIFO
%%%   queue and a set of Cassandra connection workers that are ordered
%%%   in the queue for allocation. A simple checkin / checkout API is
%%%   used to obtain a connection. If the checked out connection fails
%%%   for any reason, a supervisor will replace it and place the new
%%%   connection at the end of the queue.
%%%
%%%   A second FSM was added in v0.1.4 to allow for optional buffering
%%%   of connection requests. This approach allows spikes in the traffic
%%%   which exceed the number of available elysium sessions. Be careful
%%%   when using this feature as it masks the backpressure signal when all
%%%   sessions are occupied.
%%%
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_sup).
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
%%   by any including supervisor.
%% @end
start_link(Config) ->
    supervisor:start_link({local, ?SUPER}, ?MODULE, {Config}).
    

%%%-----------------------------------------------------------------------
%%% Internal API
%%%-----------------------------------------------------------------------

-define(CHILD(__Mod, __Args), {__Mod, {__Mod, start_link, __Args},
                               permanent, 2000, worker, [__Mod]}).
-define(SUPER(__Mod, __Args), {__Mod,  {__Mod, start_link, __Args},
                               permanent, infinity, supervisor, [__Mod]}).

-spec init({config_type()}) -> {ok, {{supervisor:strategy(), non_neg_integer(), non_neg_integer()},
                                     [supervisor:child_spec()]}}.
%% @doc
%%   Starts the supervisor which owns the Cassandra connection queue,
%%   followed by the gen_fsm which handles connnection queue status calls,
%%   and one supervisor of all Cassandra node connections. They are
%%   rest_for_one so that the queue is guaranteed to exist before
%%   any connections are created.
%% @end
init({Config}) ->
    true        = elysium_config:is_valid_config (Config),
    Buffer_Sup  = ?SUPER(elysium_buffer_sup,     [Config]),
    Queue_Proc  = ?CHILD(elysium_queue,          [Config]),
    Conn_Sup    = ?SUPER(elysium_connection_sup,       []),
    {ok, {{rest_for_one, 10, 10}, [Buffer_Sup, Queue_Proc, Conn_Sup]}}.
