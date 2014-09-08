elysium [![Build Status](https://travis-ci.org/jaynel/elysium.svg)](https://travis-ci.org/jaynel/elysium)
=======================================================================================================

Elysium is the heavenly afterlife location where Cassandra was rewarded for the suffering she endured on earth. This project will hopefully make life easier for erlang projects which need multiple connections to a Cassandra server. The limited resource of socket connections is maintained in a FIFO queue, so that a connection may only be used by one process at a time. When the task is complete, the resource is added back to the end of the queue of available resources.

The FIFO queue is maintained by epocxy in an ets_buffer (an ets table with numerically indexed keys). The functionality is ensured by a full suite of PropEr tests running under common_test control.

Testing
-------
In order to run tests, clone and run make:

    $ git clone https://github.com/duomark/elysium
    $ cd elysium
    $ make tests

To check that the types are properly specified, make the plt and run dialyzer:

    $ make plt
    $ make dialyze

Travis CI
-----------
Builds are also enabled through travis-ci.org
