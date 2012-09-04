# ZooKeeper Benchmark

Authors: Chen Liang, Andrew Ferguson, Rodrigo Fonseca

## Build and Usage Instructions

To compile the code, run: 

	mvn -DZooKeeperVersion=<version> package

where `<version>` is a ZooKeeper version such as 3.4.3, 3.5.0-pane, etc. The
client code corresponding to the ZooKeeper version will be found using maven.

After this, run the benchmark using a configuration file:

    java -cp target/lib/*:target/* edu.brown.cs.zkbenchmark.ZooKeeperBenchmark --conf benchmark.conf

The configuration file provides the list of servers to contact and parameters
for the benchmark; please see the included example for more details. Many
configuration paramters can also be set on the command line. A `--help` option
lists the possible options.

In addition, we have included a script `runBenchmark.sh` which launches runs
of the example benchmark configuration. It requires one argument, a name for
the run, and has an optional second argument which, when set to `--gnuplot`,
plots the output files using the included gnuplot scripts.

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

## Notes

1. In the benchmark, node creation and deletion tests are done by creating a lot
of nodes at first, and then deleting them. Since each test runs for a fixed 
amount of time, there are no guarantees about the number of nodes each creates.
If there are more delete requests than create requests, the extra delete 
requests would end up not actually deleting anything. Though these requests are 
sent and processed by ZooKeeper server anyway; this could be an issue.

2. Read requests done done by ZooKeeper extremly quickly compared with write 
requests. If the time interval and threshold are not chosen appropriately, it 
could happen that when the timer awakes, all requests have already been 
finished. In this case, the output of read test doesn't reflect the actual rate 
of read requests. 
