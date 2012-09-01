#!/bin/bash

RUN_NAME="$1"
USE_GNUPLOT="$2" # if set to "--gnuplot", plot the output files

if [ "$RUN_NAME" == "" ]; then
	echo "Name for benchmark run required!"
	exit 1
fi

if [ -d "$RUN_NAME" ]; then
	echo "Benchmark run $RUN_NAME already exists!"
	exit 2
fi

mkdir "$RUN_NAME"
pushd "$RUN_NAME" 1>/dev/null

# Copy the parameters to be used
cp ../benchmark.conf .

# Record the git hash of the benchmark used
git log -1 --format="%H" > "zookeeper-benchmark.version"

# Run the benchmark
java -cp ../target/lib/*:../target/* edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark --conf benchmark.conf 2>&1 | tee "$RUN_NAME.out"

# Optionally, plot some graphs
if [ "`which gnuplot`" != "" ] && [ "$USE_GNUPLOT" == "--gnuplot" ]; then
    gnuplot ../all.plot

    if [ "`which ps2pdf`" != "" ]; then
        for i in `ls -1 *.ps`; do
            ps2pdf $i
        done
    fi
fi

popd 1>/dev/null
