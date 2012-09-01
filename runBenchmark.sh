#!/bin/bash

if [ "$1" == "" ]; then
	echo "Name of experiment required!"
	exit
fi

if [ -d "$1" ]; then
	echo "Experiment $1 already exists!"
	exit
fi

cd src
java -cp target/lib/*:target/* edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark --conf benchmark.conf 2>&1 | tee $1.out

mv *.dat ..
mv $1.out ..
cd ..

gnuplot all.plot
for i in `ls -1 *.ps`; do
	ps2pdf $i
done

mkdir $1
mv *.dat *.ps *.pdf $1.out $1
cp src/curatorTest.java $1
