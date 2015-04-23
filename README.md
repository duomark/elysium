elysium [![Build Status](https://travis-ci.org/jaynel/elysium.svg)](https://travis-ci.org/jaynel/elysium)
=======================================================================================================

Elysium is the heavenly afterlife location where Cassandra was rewarded for the suffering she endured on earth. This project will hopefully make life easier for erlang projects which need multiple connections to a Cassandra server. The limited resource of socket connections is maintained in a FIFO queue, so that a connection may only be used by one process at a time. When the task is complete, the resource is added back to the end of the queue of available resources.

Library Logic
-------------
Elysium provides a supervisor hierarchy to manage itself as an included application. Just plug the `elysium_sup` into your hierarchy.

Internally the supervisor hierarchy is a rest_for_one organization as shown in the [Supervisor Hierarchy Diagram](doc/supervision_tree.pdf). The leftmost children manage data that must always be present, the middle children are the actual implementation and the last supervisor manages dynamic connections to Cassandra. Although this library was written for use with one or more Cassandra clusters using [seestar](https://github.com/iamaleksey/seestar) as the Cassandra connection library, the only Cassandra dependencies are in `elysium_connection` (this module instantiates the actual DB socket connections, explicitly naming seestar) and `elysium_peer_handler` (which uses Cassandra CQL syntax to query the seed node for new Cassandra nodes participating in the cluster). Generalizing the approach to other databases would not be difficult.

The fundamental logic involves maintaining a queue of idle DB connections and a queue of pending requests. A DB request becomes pending only when the queue of idle connections is empty. Normally a call to `elyisum_buffering_strategy:with_connection/4,5` is sufficient to query the DB. This call determines the installed behaviour for the queues, then checks out an idle connection, performs the DB request and checks the connection back in. No other assumptions about the implementation of the queue, the means of checkin/checkout or other characteristics are made, besides that a checked out connection is not idle and can only be used for a single request.

When there are no idle connections, the request is checked in to the pending requests queue. During connection checkin, the pending requests queue is consulted for immediate reuse prior to handing a connection to the idle connection queue. Pending requests leave the clock running for a timeout while they are waiting for an available connection. If the pending request is checked out of its queue, but it has either crashed or the time has run out for the query, the request is discarded and the connection attempts to get another pending request. If max_retries are used and no valid pending_request is found, the connection is checked in as idle without attempting to fetch any more pending requests from the queue.

The two queues are implemented as behaviour instances of `elysium_buffering_strategy`. Both `elysium_bs_parallel` and `elysium_bs_serial` are available as examples, but the parallel implementation relies on a FIFO `ets_buffer` from [epocxy](https://github.com/duomark/epocxy/) which has concurrency bugs. Right now you should stick with the use of `elysium_bs_serial` for both the connection and pending requests queues.

To safely reconfigure the number or location of connections, use `elysium_queue:deactivate/0` followed by `elysium_queue:activate/0` after making changes. These two operations close all active connections and then reopen the connections respectively. The termination is abrupt as the code fetches all `elysium_connection_sup` children and terminates them, so a bit of quiescence is recommended before the restart sequence.

Top-Level Supervisors
---------------------
The following children are maintained as the top-level rest_for_one children of `elysium_sup`:

  1. **elysium_buffer_sup**: Purely an ets table owner
  1. **elysium_queue**: An API for querying the status and activating/deactivating elysium
  1. **elysium_lb_queue**: A round-robin queue of DB hostname/port pairs used for creating new connections
  1. **elysium_peer_handler**: A Cassandra seed query mechanism to discover Cassandra server nodes added and removed from the cluster
  1. **elysium_session_queue**: A `elysium_serial_queue` of all idle connections to the database
  1. **elysium_pending_queue**: An `elysium_serial_queue` of all pending requests when no idle connections exist
  1. **elysium_connection_sup**: The only supervisor which manages live connections to the DB cluster

Whenever new connections are needed, they are obtained from the `elysium_lb_queue` by connecting to the next available host in the round-robin queue. After connecting, the host is added to the end of the queue for continuous ring-like action. If the connection to the host times out or has other errors, it is put back at the end of the queue and the next host is tried. This distributes new connections across the DB cluster while skipping those nodes which are not responsive.

The session queue is used to checkin/checkout idle connections. On checkin, there is a random probability (default of 100,000 per 1 Billion attempts) that the connection will "decay". A decayed connection is closed and replaced with a new connection. The purpose of decay is to allow connections to dynamically migrate around the DB cluster so that any staleness, memory fragmentation or other possible latent bugs are expunged before they cause serious damage. If there are problems on the DB cluster, the built-in stochastic jitter should make communications more resilient and balanced across the DB cluster.

Testing
-------
All testing is currently done within the Tigertext application environment. Tests for this library will be added later.

In order to run tests, clone and run make:

    $ git clone https://github.com/duomark/elysium
    $ cd elysium
    $ make tests

To check that the types are properly specified, make the plt and run dialyzer:

    $ make plt
    $ make dialyze

Travis CI
=========
[Travis-CI](http://about.travis-ci.org/) provides Continuous Integration. Travis automatically builds and runs the unit tests whenever the code is modified. The status of the current build is shown in the image badge at the top of this page.

Integration with Travis is provided by the [.travis.yml file](https://raw.github.com/duomark/erlangsp/master/.travis.yml). The automated build is run on R16B03.
