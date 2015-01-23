%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_bs_parallel is a buffering strategy which enforces serial
%%%   initiation of Cassandra session queries, although the duration of
%%%   a query will vary so they are not guaranteed to execute in serial
%%%   order. This approach allows spikes in the traffic which exceed the
%%%   number of availabel elysium sessions. The buffer is maintained as
%%%   a FIFO ets_buffer, so it has overhead when unloading expired pending
%%%   requests when compared to a LIFO buffer for the same task. All
%%%   requests attempt to fetch an idle session before being added to
%%%   the end of the pending queue.
%%%
%%% @since 0.1.5
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_bs_parallel).
-author('jay@duomark.com').

-behaviour(elysium_buffering_strategy).
-behaviour(elysium_buffering_audit).

%% Buffering Strategy API
-export([
         idle_connection_count/1,
         checkin_connection/2,
         checkout_connection/1,
         checkout_connection_error/4,

         pending_request_count/1,
         checkin_pending_request/3,
         checkout_pending_request/1,
         checkout_pending_error/4,

         queue_status/1,
         queue_status_reset/1
        ]).

%% Buffering audit API
-export([
         audit_count/3,
         audit_count_init/3
        ]).

-include("elysium_types.hrl").
-include("elysium_audit_types.hrl").

-record(audit_parallel_counts, {
          count_type_key            :: audit_custom_counts_key(),
          pending_ets_errors    = 0 :: audit_count(),
          pending_missing_data  = 0 :: audit_count(),
          session_ets_errors    = 0 :: audit_count(),
          session_missing_data  = 0 :: audit_count()
         }).

-type audit_parallel_counts() :: #audit_parallel_counts{}.
-export_type([audit_parallel_counts/0]).

-define(SERVER, ?MODULE).


%%%-----------------------------------------------------------------------
%%% Buffering Strategy API
%%%-----------------------------------------------------------------------

-spec idle_connection_count(connection_queue_name()) -> max_connections().
%% @doc Get the current number of idle connections.
idle_connection_count(Connection_Queue) ->
    ets_buffer:num_entries_dedicated(Connection_Queue).

-spec checkin_connection(connection_queue_name(), cassandra_connection()) -> max_connections().
%% @doc Add free connection to the end of the ets_buffer queue.
checkin_connection(Connection_Queue, {_Node, _Connection_Id} = Connection_Data) ->
    ets_buffer:write_dedicated(Connection_Queue, Connection_Data).

-spec checkout_connection(connection_queue_name()) -> connection_baton() | none_available | any().
%% @doc Get a free conneciton from the front of the ets_buffer queue.
checkout_connection(Connection_Queue) ->
    case ets_buffer:read_dedicated(Connection_Queue) of
        []           -> none_available;
        [Connection] -> Connection;
        Error        -> Error
    end.

-spec checkout_connection_error(config_type(), buffering_strategy_module(), connection_queue_name(), Error)
                               -> retry | Error when Error::any().
%% @doc Report errors when checking out a connection failed.
%% Race condition with checkin, try again...
%% (When this happens, a connection is left behind in the queue and will never get reused!)
checkout_connection_error(Config, ?MODULE, Connection_Queue, {missing_ets_data, Connection_Queue, Read_Loc}) ->
    _ = elysium_buffering_audit:audit_count(Config, ?MODULE, session_missing_data),
    lager:error("Missing ETS data reading ~p at location ~p~n", [Connection_Queue, Read_Loc]),
    retry;
checkout_connection_error(Config, ?MODULE, Connection_Queue, Error) ->
    elysium_buffering_audit:audit_count(Config, ?MODULE, session_ets_errors),
    lager:error("Connection queue ~p buffer error: ~9999p~n", [Connection_Queue, Error]),
    Error.


-spec pending_request_count(requests_queue_name()) -> pending_count().
%% @doc Get the current number of Pending Requests in the ets_buffer.
pending_request_count(Pending_Queue) ->
    ets_buffer:num_entries_dedicated(Pending_Queue).

-spec checkin_pending_request(requests_queue_name(), reference(), erlang:timestamp()) -> pending_count().
%% @doc Put the Pending Request at the end of the elysium_serial_queue and return the new size.
checkin_pending_request(Pending_Queue, Sid_Reply_Ref, Start_Time) ->
    ets_buffer:write_dedicated(Pending_Queue, {{self(), Sid_Reply_Ref}, Start_Time}).

-spec checkout_pending_request(requests_queue_name()) -> none_available | pending_request_baton() | any().
%% @doc Put the Pending Request at the end of the elysium_serial_queue and return the new size.
checkout_pending_request(Pending_Queue) ->
    case ets_buffer:read_dedicated(Pending_Queue) of
        []                -> none_available;
        [Pending_Request] -> Pending_Request;
        Error             -> Error
    end.

-spec checkout_pending_error(config_type(), buffering_strategy_module(), requests_queue_name(), Error)
                            -> Error when Error::any().
%% Race condition with pend_request, try again...
%% (When this happens, a pending request is left behind in the queue and will timeout)
checkout_pending_error(Config, ?MODULE, Pending_Queue, {missing_ets_data, Pending_Queue, Read_Loc}) ->
    _ = elysium_buffering_audit:audit_count(Config, ?MODULE, pending_missing_data),
    lager:error("Missing ETS data reading ~p at location ~p~n", [Pending_Queue, Read_Loc]),
    retry;
checkout_pending_error(Config, ?MODULE, Pending_Queue, Error) ->
    _ = elysium_buffering_audit:audit_count(Config, ?MODULE, pending_ets_errors),
    lager:error("Pending requests queue ~p buffer error: ~9999p~n", [Pending_Queue, Error]),
    Error.


-spec queue_status       (connection_queue_name() | requests_queue_name()) -> {status, proplists:proplist()}.
-spec queue_status_reset (connection_queue_name() | requests_queue_name()) -> {status, proplists:proplist()}.

queue_status       (_Queue_Name) -> {status, []}.
queue_status_reset (_Queue_Name) -> {status, []}.


%%%-----------------------------------------------------------------------
%%% Custom audit count functions
%%%-----------------------------------------------------------------------

-spec audit_count_init(config_type(), audit_counts_ets_name(), audit_custom_counts_key()) -> boolean().
-spec audit_count(audit_counts_ets_name(), audit_custom_counts_key(), audit_custom_event()) -> audit_count().

audit_count_init(_Config, Audit_Name, Audit_Custom_Key) ->
    ets:insert_new(Audit_Name, #audit_parallel_counts{count_type_key=Audit_Custom_Key}).

audit_count(Audit_Name, Audit_Custom_Key, Custom_Type) ->
    Counter_Pos = case Custom_Type of
                      pending_ets_errors    -> #audit_parallel_counts.pending_ets_errors;
                      pending_missing_data  -> #audit_parallel_counts.pending_missing_data;
                      session_ets_errors    -> #audit_parallel_counts.session_ets_errors;
                      session_missing_data  -> #audit_parallel_counts.session_missing_data
                  end,
    ets:update_counter(Audit_Name, Audit_Custom_Key, {Counter_Pos, 1}).
