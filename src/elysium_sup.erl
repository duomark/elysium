%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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

-define(CHILD(__Mod, __Args),         {__Mod,  {__Mod, start_link, __Args}, permanent, 2000, worker, [__Mod]}).
-define(CHILD(__Name, __Mod, __Args), {__Name, {__Mod, start_link, __Args}, permanent, 2000, worker, [__Mod]}).
-define(SUPER(__Mod, __Args), {__Mod, {__Mod, start_link, __Args}, permanent, infinity, supervisor,  [__Mod]}).

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
    true        = elysium_config:is_valid_config(Config),
    Lb_Queue_Name = elysium_config:load_balancer_queue(Config),
    Session_Queue_Name = elysium_config:session_queue_name  (Config),
    Pending_Queue_Name = elysium_config:requests_queue_name (Config),
    Buffer_Sup  = ?SUPER(elysium_buffer_sup,       [Config]),
    Queue_Proc  = ?CHILD(elysium_queue,            [Config]),
    Discovery_Proc = ?CHILD(elysium_peer_handler,  [Config]),
    Conn_Sup    = ?SUPER(elysium_connection_sup,         []),

    Serial_Session_Queue = ?CHILD(elysium_serial_sessions, elysium_serial_queue, [Session_Queue_Name]),
    Serial_Pending_Queue = ?CHILD(elysium_serial_pendings, elysium_serial_queue, [Pending_Queue_Name]),
    Serial_LB_Queue = ?CHILD(elysium_serial_lb, elysium_lb_queue, [Config, Lb_Queue_Name]),

    {ok, {{rest_for_one, 10, 10},
          [Buffer_Sup, Queue_Proc,                       % Ets owner and status reporter
           Serial_LB_Queue,                              % Queue for peer nodes
           Discovery_Proc,                               % Detects peer nodes and refreshes the buffers
           Serial_Pending_Queue, Serial_Session_Queue,   % Queue gen_servers for serial option
           Conn_Sup]}}.                                  % Connection worker supervisor
