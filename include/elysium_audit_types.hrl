-type audit_counts_ets_name()   :: atom().
-type audit_connection_key()    :: {buffering_strategy_module(), connection_id()}.
-type audit_std_counts_key()    :: {buffering_strategy_module(), counts}.
-type audit_custom_counts_key() :: {buffering_strategy_module(), custom_counts}.
-type audit_counts_key()        :: audit_std_counts_key() | audit_custom_counts_key().

-type audit_timestamp()         :: binary().
-type audit_count()             :: non_neg_integer().

-type audit_custom_counts()     :: any().
-type audit_custom_event()      :: atom().
-type audit_count_event()       :: pending_dead
                                 | pending_timeouts
                                 | session_dead
                                 | session_decay
                                 | session_timeouts
                                 | session_wrong
                                 | worker_errors
                                 | worker_timeouts.
