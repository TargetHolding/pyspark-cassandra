#export IPYTHON_OPTS="notebook"

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

PYSPARK_DRIVER_PYTHON=ipython \
	$DIR/lib/spark-1.4.1-bin-hadoop2.6/bin/pyspark \
	--conf spark.cassandra.connection.host="localhost" \
	--driver-memory 2g \
	--master local[*] \
	--jars $DIR/target/scala-2.10/pyspark-cassandra-assembly-0.2.0.jar \
	--py-files $DIR/target/pyspark_cassandra-0.2.0-py2.7.egg \
	$@

