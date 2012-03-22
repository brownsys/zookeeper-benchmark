#!/bin/bash

if [ "$1" == "" ]; then
	echo "Name of experiment required!"
	exit
fi

if [ -d "$1" ]; then
	echo "Experiment $1 already exists!"
	exit
fi

cd java
java -Djava.ext.dirs=lib curatorTest 200 14000 5000 30000 0 2>&1 | tee $1.out

mv *.dat ..
mv $1.out ..
cd ..

gnuplot all.plot

mkdir $1
mv *.dat *.ps $1.out $1
