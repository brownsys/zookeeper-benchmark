#!/bin/bash

cd `dirname $0`

RUN_NAME="$1"
CONF_PATH="$2" # optional; we'll check if it's --gnuplot below
USE_GNUPLOT="$3" # if set to "--gnuplot", plot the output files

CONF_DEFAULT_PATH="benchmark.conf"

if [ "$RUN_NAME" == "" ]; then
	echo "Name for benchmark run required!"
	exit 1
fi

if [ -d "$RUN_NAME" ]; then
	echo "Benchmark run $RUN_NAME already exists!"
	exit 2
fi

# Some ad-hoc argument parsing...

if [ "$CONF_PATH" == "--gnuplot" ]; then
    USE_GNUPLOT="$CONF_PATH"
    CONF_PATH="$CONF_DEFAULT_PATH"
elif [ "$CONF_PATH" == "" ]; then
    CONF_PATH="$CONF_DEFAULT_PATH"
fi

# Time to start

mkdir "$RUN_NAME"

if [ ! -f "$CONF_PATH" ]; then
    echo "Configuration file $CONF_PATH does not exist!"
    exit 3
else
    echo "Using configuration: $CONF_PATH"
fi

# Copy the parameters to be used
CONF_FILE=`basename $CONF_PATH`
cp $CONF_PATH $RUN_NAME/$CONF_FILE

pushd "$RUN_NAME" 1>/dev/null

# Record the git hash of the benchmark used
git log -1 --format="%H" > "zookeeper-benchmark.version"

# Run the benchmark
java -cp ../target/lib/*:../target/* edu.brown.cs.zkbenchmark.ZooKeeperBenchmark --conf $CONF_FILE 2>&1 | tee "$RUN_NAME.out"

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
