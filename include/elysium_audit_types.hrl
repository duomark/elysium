-type audit_connection_key() :: {buffering_strategy_module(), connection_id()}.
-type audit_timestamp()      :: binary().
-type audit_count()          :: non_neg_integer().
-type audit_count_event()    :: pending_dead
                              | pending_timeouts
                              | session_dead
                              | session_decay
                              | session_timeouts
                              | session_wrong
                              | worker_errors
                              | worker_timeouts.
