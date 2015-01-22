%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
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

%% External API for audit reporting.
-export([get_audit_count/1,
         get_audit_count_reset/1]).

%% Exported functions for audit counting behaviours.
-export([
         audit_count_init/2,
         audit_count/3,

         audit_data_init/3,
         audit_data_checkin/3,
         audit_data_pending/3,
         audit_data_checkout/3,
         audit_data_delete/3,

         timestamp/0
        ]).

-include("elysium_types.hrl").
-include("elysium_audit_types.hrl").

%% Callbacks required for mantaining telemetry statistics.
-callback audit_count(audit_counts_ets_name(), audit_custom_counts_key(), audit_custom_event()) -> audit_count().
-callback audit_count_init(config_type(), audit_counts_ets_name(), audit_custom_counts_key()) -> boolean().

-record(elysium_audit_counts, {
          count_type_key            :: audit_std_counts_key(),
          pending_dead          = 0 :: audit_count(),
          pending_timeouts      = 0 :: audit_count(),
          session_dead          = 0 :: audit_count(),
          session_decay         = 0 :: audit_count(),
          session_timeouts      = 0 :: audit_count(),
          session_wrong         = 0 :: audit_count(),
          worker_errors         = 0 :: audit_count(),
          worker_timeouts       = 0 :: audit_count(),
          custom_counts = undefined :: audit_custom_counts()
         }).

-type elysium_audit_counts() :: #elysium_audit_counts{}.

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

-export_type([elysium_audit/0, elysium_audit_counts/0]).


%%%-----------------------------------------------------------------------
%%% Audit counts across all connections
%%%-----------------------------------------------------------------------

-spec audit_count_init (config_type(), buffering_strategy_module()) -> true.
-spec audit_count      (config_type(), buffering_strategy_module(),
                        audit_count_event() | audit_custom_event()) -> audit_count().

audit_count_init(Config, BS_Module) ->
    Audit_Key        = {BS_Module, counts},
    Audit_Custom_Key = {BS_Module, custom_counts},
    Audit_Name       = elysium_config:audit_ets_name(Config),
    true = BS_Module:audit_count_init(Config, Audit_Name, Audit_Custom_Key),
    true = ets:insert_new(Audit_Name, #elysium_audit_counts{count_type_key=Audit_Key}).

audit_count(Config, BS_Module, Type) ->
    Audit_Key   = {BS_Module, counts},
    Audit_Name  = elysium_config:audit_ets_name(Config),
    case Type of
        pending_dead      -> count(Audit_Name, Audit_Key, #elysium_audit_counts.pending_dead     );
        pending_timeouts  -> count(Audit_Name, Audit_Key, #elysium_audit_counts.pending_timeouts );
        session_dead      -> count(Audit_Name, Audit_Key, #elysium_audit_counts.session_dead     );
        session_decay     -> count(Audit_Name, Audit_Key, #elysium_audit_counts.session_decay    );
        session_timeouts  -> count(Audit_Name, Audit_Key, #elysium_audit_counts.session_timeouts );
        session_wrong     -> count(Audit_Name, Audit_Key, #elysium_audit_counts.session_wrong    );
        worker_errors     -> count(Audit_Name, Audit_Key, #elysium_audit_counts.worker_errors    );
        worker_timeouts   -> count(Audit_Name, Audit_Key, #elysium_audit_counts.worker_timeouts  );
        Custom_Type when is_atom(Custom_Type) ->
            Audit_Custom_Key = {BS_Module, custom_counts},
            BS_Module:audit_count(Audit_Name, Audit_Custom_Key, Custom_Type)
    end.

count(Audit_Name, Audit_Key, Counter_Pos) ->
    ets:update_counter(Audit_Name, Audit_Key, {Counter_Pos, 1}).


-spec get_audit_count       (config_type()) -> proplists:proplist().
-spec get_audit_count_reset (config_type()) -> proplists:proplist().

get_audit_count(Config) ->
    Count_Flds = count_fields(),
    Count_Ops  = [{Pos, 0} || {_Label, Pos} <- Count_Flds],
    return_counts_proplist(Config, Count_Flds, Count_Ops).

get_audit_count_reset(Config) ->
    Count_Flds = count_fields(),
    Count_Ops  = [{Pos, 0, 0, 0} || {_Label, Pos} <- Count_Flds],
    return_counts_proplist(Config, Count_Flds, Count_Ops).

return_counts_proplist(Config, Count_Flds, Count_Ops) ->
    {_Buffering, BS_Module} = elysium_connection:get_buffer_strategy_module(Config),
    Audit_Key  = {BS_Module, counts},
    Audit_Name = elysium_config:audit_ets_name(Config),
    Raw_Counts = ets:update_counter(Audit_Name, Audit_Key, Count_Ops),
    lists:zipwith(fun({Label, _Pos}, Count) -> {Label, Count} end, Count_Flds, Raw_Counts).

count_fields() ->    
    [{pending_dead,     #elysium_audit_counts.pending_dead},
     {pending_timeouts, #elysium_audit_counts.pending_timeouts},
     {session_dead,     #elysium_audit_counts.session_dead},
     {session_decay,    #elysium_audit_counts.session_decay},
     {session_timeouts, #elysium_audit_counts.session_timeouts},
     {session_wrong,    #elysium_audit_counts.session_wrong},
     {worker_errors,    #elysium_audit_counts.worker_errors},
     {worker_timeouts,  #elysium_audit_counts.worker_timeouts}].


%%%-----------------------------------------------------------------------
%%% Audit counts for each connection
%%%-----------------------------------------------------------------------

-spec audit_data_init     (config_type(), buffering_strategy_module(), connection_id()) -> true.
-spec audit_data_checkin  (config_type(), buffering_strategy_module(), connection_id()) -> audit_count().
-spec audit_data_pending  (config_type(), buffering_strategy_module(), connection_id()) -> audit_count().
-spec audit_data_checkout (config_type(), buffering_strategy_module(), connection_id()) -> audit_count().
-spec audit_data_delete   (config_type(), buffering_strategy_module(), connection_id()) -> true.

audit_data_init(Config, BS_Module, Connection_Id) ->
    Audit_Key  = {BS_Module, Connection_Id},
    Audit_Name = elysium_config:audit_ets_name(Config),
    true = ets:insert_new(Audit_Name, #elysium_audit{connection_id_key=Audit_Key, init_checkin=timestamp()}).

audit_data_checkin(Config, BS_Module, Connection_Id) ->
    Audit_Key  = {BS_Module, Connection_Id},
    Audit_Name = elysium_config:audit_ets_name(Config),
    case ets:lookup_element(Audit_Name, Audit_Key, #elysium_audit.init_checkin) of
        undefined  -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.init_checkin, timestamp()});
        _Timestamp -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.last_checkin, timestamp()})
    end,
    ets:update_counter(Audit_Name, Audit_Key, {#elysium_audit.num_checkins, 1}).

audit_data_pending(Config, BS_Module, Connection_Id) ->
    Audit_Key  = {BS_Module, Connection_Id},
    Audit_Name = elysium_config:audit_ets_name(Config),
    case ets:lookup_element(Audit_Name, Audit_Key, #elysium_audit.init_pending) of
        undefined  -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.init_pending, timestamp()});
        _Timestamp -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.last_pending, timestamp()})
    end,
    ets:update_counter(Audit_Name, Audit_Key, {#elysium_audit.num_pendings, 1}).

audit_data_checkout(Config, BS_Module, Connection_Id) ->
    Audit_Key  = {BS_Module, Connection_Id},
    Audit_Name = elysium_config:audit_ets_name(Config),
    case ets:lookup_element(Audit_Name, Audit_Key, #elysium_audit.init_checkout) of
        undefined  -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.init_checkout, timestamp()});
        _Timestamp -> ets:update_element(Audit_Name, Audit_Key, {#elysium_audit.last_checkout, timestamp()})
    end,
    ets:update_counter(Audit_Name, Audit_Key, {#elysium_audit.num_checkouts, 1}).

audit_data_delete(Config, BS_Module, Connection_Id) ->
    Audit_Name = elysium_config:audit_ets_name(Config),
    ets:delete(Audit_Name, {BS_Module, Connection_Id}).


%%%-----------------------------------------------------------------------
%%% Timestamps are used by all audit functions
%%%-----------------------------------------------------------------------

-spec timestamp() -> audit_timestamp().
timestamp() ->
    TS = {_,_,Micro} = os:timestamp(),
    {{Year,Month,Day},{Hour,Minute,Second}} = calendar:now_to_universal_time(TS),
    Month_Str = element(Month,{"Jan","Feb","Mar","Apr","May","Jun","Jul", "Aug","Sep","Oct","Nov","Dec"}),
    Time_Str  = io_lib:format("~4w-~s-~2wT~2w:~2..0w:~2..0w.~6..0w",
                              [Year, Month_Str, Day, Hour, Minute, Second, Micro]),
    list_to_binary(Time_Str).
