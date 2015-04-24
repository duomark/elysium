Releases
========

 Vsn  |   Date     |   Desc
------|------------|----------
[0.2.3](#0.2.3) | In Progress | [Add documentation and diagrams](#0.2.3)
[0.2.2](#0.2.2) | 2015/04/16 | [Add app.config configuration option](#0.2.2)
[0.2.1](#0.2.1) | 2015/01/21 | [Dialyzer fixes; lb and serial queue mirrored](#0.2.1)
[0.2.0](#0.2.0) | 2014/12/31 | [Add node discovery and load balance updates](#0.2.0)
[0.1.6](#0.1.6) | 2014/10/17 | [Cleanup queue ownership and status interface](#0.1.6)
[0.1.5](#0.1.5) | 2014/10/15 | [Serial buffering of pending requests](#0.1.5)
[0.1.4](#0.1.4) | 2014/10/06 | [Allow enabled/disabled elysium_queue on startup](#0.1.4)
[0.1.3](#0.1.3) | 2014/10/02 | [Add consistency to all CQL requests](#0.1.3)
[0.1.2](#0.1.2) | 2014/10/01 | [Compiled config with round-robin load balancing](#0.1.2)
[0.1.1](#0.1.1) | 2014/09/25 | [Session decay](#0.1.1)
[0.1.0](#0.1.0) | 2014/09/22 | [Initial release](#0.1.0)

### <a name="0.2.3"></a>0.2.3 Add documentation and diagrams

    * Add doc directory with text and diagrams
    * Eliminate elysium_session_enqueuer module
    * Remove unused elysium_queue:node_change/1
    * Add Cassandra node reporting on connection errors
    * Move pending requests earlier than connection queue in supervisor

### <a name="0.2.2"></a>0.2.2 Add app.config configuration option

    * {config_app_config, atom()} defines the app.config parameter
    * Using app.config means the atom() has to be a loaded application
    * Add some status reporting for metrics

### <a name="0.2.1"></a>0.2.1 Dialyzer fixs; lb serial queue mirrored

    * Fixed dialyzer complaints
    * Mirrored serial queue functionality in load balance queue
    * Updated copyright notices to 2015
    * Improved connection error reporting

### <a name="0.2.0"></a>0.2.0 Add node discovery and load balance updates

    * Add elysium_peer_handler to query Cassandra for cluster nodes
    * Update load balance ring buffer with newly discovered nodes
    * Add some status reporting for metrics

### <a name="0.1.6"></a>0.1.6 Cleanup queue ownership and status interface

    * Queued sessions are now {{Ip, Port}, Session_Pid} for debugging
    * Add elysium_config:is_elysium_config_enabled/1
    * Add elysium_buffer_sup to own session and pending request ets_buffers
    * Remove elysium_overload

### <a name="0.1.5"></a>0.1.5 Serial buffering of pending requests

    * Replace elysium_overload with elysium_bs_serial
    * Introduce Buffering Strategy parameter when spike uses all sessions

### <a name="0.1.4"></a>0.1.4 Allow enabled/disabled elysium_queue on startup

    * Allow elysium_queue to be disabled on startup

### <a name="0.1.3"></a>0.1.3 Add consistency to all CQL requests

    * Add max_restart_delay and stochastic restart to avoid thundering herds
    * Add Consistency parameter to all CQL requests

### <a name="0.1.2"></a>0.1.2 Compiled config with round-robin load balancing

    * Allow multiple Cassandra nodes with round-robin connections
    * Allow multiple options for configuration data

      * Accept vbisect dictionary to use for all config calls (read-only binary)
      * Use compiled module behaviour 'elysium_config' for concurrency

### <a name="0.1.1"></a>0.1.1 Session decay

    * Added N chances in 1M uses that a session will be replaced

### <a name="0.1.0"></a>0.1.0 Initial release

    * FIFO queue of supervised seestar Cassandra sessions