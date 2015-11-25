#!/bin/bash

make dist-python

while inotifywait -e modify -e create -e delete -r python
do
  make dist-python
  echo "----------------------------------------"
  echo "   pyspark_cassandra (python) built"
  echo "   "`date`
  echo "----------------------------------------"
done
