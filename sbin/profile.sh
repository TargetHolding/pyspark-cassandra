DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

$DIR/pyspark_cassandra.sh --conf spark.python.profile=true $@

