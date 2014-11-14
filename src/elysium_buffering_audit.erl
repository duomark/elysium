%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Buffering audit is used to count statistics of the internals
%%%   of a buffering strategy behaviour. These functions must be
%%%   implemented so that statistics across behaviours are comparable.
%%%
%%% @since 0.1.7
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_buffering_audit).
-author('jay@duomark.com').

-export([
         timestamp/0,
         audit_count_init/3,
         audit_data_checkin/3,
         audit_data_pending/3,
         audit_data_checkout/3,
         audit_data_delete/3
        ]).

-include("elysium_types.hrl").
-include("elysium_audit_types.hrl").

%% Individual counts for connection, pending, worker and other counts across all connections.
-callback audit_count(config_type(), audit_count_event() | atom()) -> audit_count().

-record(elysium_audit, {
          connection_id_key :: audit_connection_key(),

          init_checkin      :: audit_timestamp(),
          last_checkin      :: audit_timestamp(),
          num_checkins  = 0 :: audit_count(),

          init_checkout     :: audit_timestamp(),
          last_checkout     :: audit_timestamp(),
          num_checkouts = 0 :: audit_count(),

          init_pending      :: audit_timestamp(),
          last_pending      :: audit_timestamp(),
          num_pendings  = 0 :: audit_count()
         }).

-type elysium_audit() :: #elysium_audit{}.
-export_type([elysium_audit/0]).

-spec timestamp() -> audit_timestamp().
timestamp() ->
    TS = {_,_,Micro} = os:timestamp(),
    {{Year,Month,Day},{Hour,Minute,Second}} = calendar:now_to_universal_time(TS),
    Month_Str = element(Month,{"Jan","Feb","Mar","Apr","May","Jun","Jul", "Aug","Sep","Oct","Nov","Dec"}),
    Time_Str  = io_lib:format("~4w-~s-~2wT~2w:~2..0w:~2..0w.~6..0w",
                              [Year, Month_Str, Day, Hour, Minute, Second, Micro]),
    list_to_binary(Time_Str).

%% Counts maintained per connection.
-spec audit_count_init    (config_type(), buffering_strategy_module(), connection_id()) -> true.
-spec audit_data_checkin  (config_type(), buffering_strategy_module(), connection_id()) -> audit_count().
-spec audit_data_pending  (config_type(), buffering_strategy_module(), connection_id()) -> audit_count().
-spec audit_data_checkout (config_type(), buffering_strategy_module(), connection_id()) -> audit_count().
-spec audit_data_delete   (config_type(), buffering_strategy_module(), connection_id()) -> true.

audit_count_init(Config, BS_Module, Connection_Id) ->
    Audit_Key  = {BS_Module, Connection_Id},
    Audit_Name = elysium_config:audit_ets_name(Config),
    true = ets:insert_new(Audit_Name, #elysium_audit{connection_id_key=Audit_Key, init_checkin=timestamp()}).

audit_data_checkin(Config, BS_Module, Connection_Id) ->
    Audit_Name = elysium_config:audit_ets_name(Config),
    Audit_Key  = {BS_Module, Connection_Id},
    case ets:lookup_element(Audit_Name, Audit_Key, #elysium_audit.init_checkin) of
        undefined  -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.init_checkin, timestamp()});
        _Timestamp -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.last_checkin, timestamp()})
    end,
    ets:update_counter(Audit_Name, Audit_Key, {#elysium_audit.num_checkins, 1}).

audit_data_pending(Config, BS_Module, Connection_Id) ->
    Audit_Name = elysium_config:audit_ets_name(Config),
    Audit_Key  = {BS_Module, Connection_Id},
    case ets:lookup_element(Audit_Name, Audit_Key, #elysium_audit.init_pending) of
        undefined  -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.init_pending, timestamp()});
        _Timestamp -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.last_pending, timestamp()})
    end,
    ets:update_counter(Audit_Name, Audit_Key, {#elysium_audit.num_pendings, 1}).

audit_data_checkout(Config, BS_Module, Connection_Id) ->
    Audit_Name = elysium_config:audit_ets_name(Config),
    Audit_Key  = {BS_Module, Connection_Id},
    case ets:lookup_element(Audit_Name, Audit_Key, #elysium_audit.init_checkout) of
        undefined  -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.init_checkout, timestamp()});
        _Timestamp -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.last_checkout, timestamp()})
    end,
    ets:update_counter(Audit_Name, Audit_Key, {#elysium_audit.num_checkouts, 1}).

audit_data_delete(Config, BS_Module, Connection_Id) ->
    Audit_Name = elysium_config:audit_ets_name(Config),
    ets:delete(Audit_Name, {BS_Module, Connection_Id}).
