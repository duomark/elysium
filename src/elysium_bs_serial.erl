%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Elysium_bs_serial uses elysium_serial_queue for each of the session
%%%   and pending request queues. It doesn't allow any concurrency other
%%%   than these two gen_servers which internally maintain an erlang queue
%%%   of connections in the case of the session queue, and pid + reply_ref
%%%   in the case of pending CQL requests.
%%%
%%% @since 0.1.6i
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_bs_serial).
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

%% Buffering Audit API
-export([
         audit_count/3,
         audit_count_init/3
        ]).

-include("elysium_types.hrl").
-include("elysium_audit_types.hrl").

-define(SERVER, ?MODULE).


%%%-----------------------------------------------------------------------
%%% Buffering Strategy API
%%%-----------------------------------------------------------------------

-spec idle_connection_count(connection_queue_name()) -> max_connections().
%% @doc Get the current number of Cassandra Connections in the elysium_serial_queue.
idle_connection_count(Connection_Queue) ->
    elysium_serial_queue:num_entries(Connection_Queue).

-spec checkin_connection(connection_queue_name(), cassandra_connection()) -> max_connections().
%% @doc Add free connection to the end of serial queue.
checkin_connection(Connection_Queue, {_Node, _Connection_Id} = Connection_Data) ->
    elysium_serial_queue:checkin(Connection_Queue, Connection_Data).

-spec checkout_connection(connection_queue_name()) -> cassandra_connection() | none_available.
%% @doc Get free connection to the beginning of serial queue.
checkout_connection(Connection_Queue) ->
    case elysium_serial_queue:checkout(Connection_Queue) of
        empty -> none_available;
        {value, Value} -> Value
    end.

-spec checkout_connection_error(config_type(), buffering_strategy_module(), connection_queue_name(), Error)
                               -> Error when Error::any().
%% This function should never be called.
checkout_connection_error(_Config, ?MODULE, Connection_Queue, Error) ->
    lager:error("Connection queue ~p buffer error: ~9999p~n", [Connection_Queue, Error]),
    Error.


-spec pending_request_count(requests_queue_name()) -> pending_count().
%% @doc Get the current number of Pending Requests in the elysium_serial_queue.
pending_request_count(Pending_Queue) ->
    elysium_serial_queue:num_entries(Pending_Queue).

-spec checkin_pending_request(requests_queue_name(), reference(), erlang:timestamp()) -> pending_count().
%% @doc Put the Pending Request at the end of the elysium_serial_queue and return the new size.
checkin_pending_request(Pending_Queue, Sid_Reply_Ref, Start_Time) ->
    elysium_serial_queue:checkin(Pending_Queue, {{self(), Sid_Reply_Ref}, Start_Time}).

-spec checkout_pending_request(requests_queue_name()) -> connection_baton() | none_available.
%% @doc Put the Pending Request at the end of the elysium_serial_queue and return the new size.
checkout_pending_request(Pending_Queue) ->
    case elysium_serial_queue:checkout(Pending_Queue) of
        empty          -> none_available;
        {value, Value} -> Value
    end.

-spec checkout_pending_error(config_type(), buffering_strategy_module(), requests_queue_name(), Error)
                            -> Error when Error::any().
%% @doc This function should never be called since we expect no errors from checkout_pending_request/3.
checkout_pending_error(_Config, ?MODULE, Pending_Queue, Error) ->
    lager:error("Pending requests queue ~p buffer error: ~9999p~n", [Pending_Queue, Error]),
    Error.


-spec queue_status       (connection_queue_name() | requests_queue_name()) -> {status, proplists:proplist()}.
-spec queue_status_reset (connection_queue_name() | requests_queue_name()) -> {status, proplists:proplist()}.

queue_status       (Queue_Name) -> elysium_serial_queue:status       (Queue_Name).
queue_status_reset (Queue_Name) -> elysium_serial_queue:status_reset (Queue_Name).
    

%%%-----------------------------------------------------------------------
%%% Custom audit count functions
%%%-----------------------------------------------------------------------

-spec audit_count_init(config_type(), audit_counts_ets_name(), audit_custom_counts_key()) -> boolean().
-spec audit_count(audit_counts_ets_name(), audit_custom_counts_key(), audit_custom_event()) -> audit_count().

audit_count_init (_Config, _Audit_Name, _Audit_Custom_Key) -> true.
audit_count(_Audit_Name, _Audit_Custom_Key, _Custom_Type) -> 0.
