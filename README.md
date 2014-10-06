Cassandra Distributed Tests
===========================

Tests for [Apache Cassandra](http://apache.cassandra.org) clusters.

Prerequisites
------------

An up to date copy of ccm should be installed for starting and stopping Cassandra.
The tests are run using nosetests.
These tests require the datastax python driver.
A few tests still require the deprecated python CQL over thrift driver.

 * [ccm](https://github.com/pcmanus/ccm)
 * [nosetests](http://readthedocs.org/docs/nose/en/latest/)
 * [Python Driver](http://datastax.github.io/python-driver/installation.html)
 * [CQL over Thrift Driver](http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/)

Usage
-----

The tests are run by nosetests. The only thing the framework needs to know is
the location of the (compiled) sources for Cassandra. There are two options:

Use existing sources:

    CASSANDRA_DIR=~/path/to/cassandra nosetests

Use ccm ability to download/compile released sources from archives.apache.org:

    CASSANDRA_VERSION=1.0.0 nosetests

A convenient option if tests are regularly run against the same existing
directory is to set a `default_dir` in `~/.cassandra-dtest`. Create the file and
set it to something like:

    [main]
    default_dir=~/path/to/cassandra

The tests will use this directory by default, avoiding the need for any
environment variable (that still will have precedence if given though).

Existing tests are probably the best place to start to look at how to write
tests.

Each test spawns a new fresh cluster and tears it down after the test, unless
`REUSE_CLUSTER` is set to true. Then some tests will share cassandra instances. If a
test fails, the logs for the node are saved in a `logs/<timestamp>` directory
for analysis (it's not perfect but has been good enough so far, I'm open to
better suggestions).

Detailed Instructions
---------------------

See more detailed instructions in the included [INSTALL file](https://github.com/riptano/cassandra-dtest/blob/master/INSTALL.md).
