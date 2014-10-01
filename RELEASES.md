Version Releases
================

0.1.2 Compiled config with round-robin load balancing (Released Oct 1, 2014)

  * Allow multiple options for configuration data

     * Accept vbisect dictionary to use for all config calls (read-only binary)
     * Use compiled module behaviour 'elysium_config' for concurrency

  * Allow multiple Cassandra nodes with round-robin connections

0.1.1 Session decay (Released Sept 25, 2014)

  * Added N chances in 1M uses that a session will be replaced

0.1.0 Initial release (Released Sept 22, 2014)

  * FIFO queue of supervised seestar Cassandra sessions