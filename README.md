# ZooKeeper Benchmark

Authors: [Chen Liang](http://www.cs.brown.edu/~chen_liang/), [Andrew Ferguson](http://www.cs.brown.edu/~adf/), [Rodrigo Fonseca](http://www.cs.brown.edu/~rfonseca/)

Please contact adf@cs.brown.edu with any questions or comments. Patches and
additional features are more than welcome!

## Introduction

This project provides a tool to benchmark the performance of [ZooKeeper](http://zookeeper.apache.org).
It is designed to measure the per-request latency of a ZooKeeper ensemble for
a predetermined length of time (e.g., sustained handling of create requests for
30 seconds). This tool can be used to build graphs such as Figure 8 in the
paper ["ZooKeeper: Wait-free coordination for Internet-scale systems"](http://static.usenix.org/event/usenix10/tech/full_papers/Hunt.pdf),
and differs from the [ZooKeeper smoketest](https://github.com/phunt/zk-smoketest),
which submits a fixed number of operations and records the time to complete all
of them.

The benchmark exercises the ensemble's performance at handling znode reads,
repeated writes to a single znode, znode creation, repeated writes to
multiple znodes, and znode deletion. These tests can be performed with either
synchronous or asynchronous operations. The benchmark connects to each server
in the ZooKeeper ensemble using one thread per server.

In synchronous operation, each client makes a new request upon receiving the
result of the previous one. In asynchronous operation, each client thread has
a globally configurable target for the number of outstanding asynchronous
requests. If the number of outstanding requests falls below a configurable lower
bound, then new asynchronous requests are made to return to the target level.

During the benchmark, the current rate of request processing is recorded at
a configurable intermediate interval to one file per operation: READ.dat,
SETSINGLE.dat, CREATE.dat, SETMULTI.dat, and DELETE.dat. Additional output is
recorded to the log file zk-benchmark.log, including the output of the ZooKeeper
`stat` command after every test (`srst` is run before each test to reset the
statistics). Some messages are also displayed on the console, all of which can
be adjusted via the log4j.properties file.

## Build and Usage Instructions

To compile the code, run: 

	mvn -DZooKeeperVersion=<version> package

where `<version>` is a ZooKeeper version such as 3.4.3, 3.5.0-pane, etc. The
client code corresponding to the ZooKeeper version will be found using maven.

After this, run the benchmark using a configuration file:

    java -cp target/lib/*:target/* edu.brown.cs.zkbenchmark.ZooKeeperBenchmark --conf benchmark.conf

The configuration file provides the list of servers to contact and parameters
for the benchmark; please see the included example for more details. Many
configuration parameters can also be set on the command line. A `--help` option
lists the possible options:

<pre>
Option (* = required)           Description                            
---------------------           -----------                            
* --conf                        configuration file (required)          
--help                          print this help statement              
--interval <Integer>                     interval between rate measurements     
--lbound <Integer>                       lowerbound for the number of operations
--ops <Integer>                          total number of operations             
--sync <Boolean>                         sync or async test                     
--time <Integer>                         time tests will run for (milliseconds)
</pre>

In addition, we have included a script `runBenchmark.sh` which launches runs
of the example benchmark configuration. It requires one argument, a name for
the run. A path to a configuration file can be provided as an optional second
argument. Finally, if the last argument is set to `--gnuplot`, the script plots
the rate output files using the included gnuplot script, all.plot. A second
gnuplot script, multi.plot, can be used to compare two runs named "pre" and
"post".

## Eclipse Development

As a simple Maven project, our benchmark can easily be developed using Eclipse.
It is necessary to first set the M2_REPO variable for your workspace (This
command only needs to be executed once per workspace):

	mvn -Declipse.workspace=<path-to-eclipse-workspace> eclipse:configure-workspace

Next, install the libraries and create the Eclipse project files:

	mvn -DZooKeeperVersion=<version> install -DskipTests
	mvn -DZooKeeperVersion=<version> eclipse:eclipse

You can now import the project into Eclipse using, File > Import > Existing
Projects into Workspace.

If you wish to view the source or JavaDocs of the benchmark's dependencies, you
add `-DdownloadSources=true` or `-DdownloadJavadocs=true` when creating the
Eclipse project files in the final step.

Internally, this project uses the [Netflix Curator](https://github.com/Netflix/curator)
library to wrap the ZooKeeper client API.

## Notes

1. In the benchmark, node creation and deletion tests are done by creating nodes
in the first test, and then deleting them in the second. Since each test runs
for a fixed amount of time, there are no guarantees about the number of nodes
created in the first one. If there are more delete requests than create
requests, the extra delete requests will not actually deleting anything. These
requests are sent to, and processed by, the ZooKeeper server, which may affect
the average per-request latency reported by the test. Examining the
intermediately-reported request rates will provide more accurate information.

2. Read requests are handled by ZooKeeper more quickly than write requests. If
the time interval and threshold are not chosen appropriately, it could happen
that when the timer awakes, all requests have already been finished. In this
case, the output of the read test doesn't reflect the actual rate of read
requests the ensemble could support. As with the previous concern, examining
the intermediately-reported request rates will provide better insight.

## License

Ths ZooKeeper benchmark is provided under the 3-clause BSD license. See the
file LICENSE for more details.
