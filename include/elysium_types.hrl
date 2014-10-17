-type config_type() :: {config_mod, module()}
                     | {vbisect,    vbisect:bindict()}.

-type lb_queue_name()       :: ets_buffer:buffer_name().
-type session_queue_name()  :: ets_buffer:buffer_name().
-type requests_queue_name() :: ets_buffer:buffer_name().
-type host_list()           :: [{Ip_Addr::string(), Port::pos_integer()}].
-type timeout_in_ms()       :: pos_integer().      %% in milliseconds
-type max_sessions()        :: pos_integer().
-type max_retries()         :: non_neg_integer().
-type decay_prob()          :: non_neg_integer().  %% number of chances in 1M of death

-type wait_for_session_error() :: {wait_for_session_timeout, pos_integer()}
                                | {wait_for_session_error,   any()}.

-type worker_reply_error()     :: {worker_reply_timeout,     pos_integer()}
                                | {worker_reply_error,       any()}.

-type pend_request_error()     :: ets_buffer:buffer_error()
                                | wait_for_session_error()
                                | worker_reply_error().
