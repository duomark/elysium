-type config_type() :: {config_mod, module()}
                     | {vbisect,    vbisect:bindict()}.

-type lb_queue_name()       :: ets_buffer:buffer_name().
-type session_queue_name()  :: ets_buffer:buffer_name().
-type host_list()           :: [{Ip_Addr::string(), Port::pos_integer()}].
-type max_sessions()        :: pos_integer().
-type max_retries()         :: non_neg_integer().
-type decay_prob()          :: non_neg_integer().
