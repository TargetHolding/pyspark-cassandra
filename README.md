PySpark Cassandra
=================

PySpark Cassandra brings back the fun in working with Cassandra data in PySpark.

This module provides python support for Apache Spark's Resillient Distributed Datasets from Apache Cassandra CQL rows using [Cassandra Spark Connector](https://github.com/datastax/spark-cassandra-connector) within PySpark, both in the interactive shell and in python programmes submitted with spark-submit.


Building
--------

A Java / JVM library as well as a python library is required to use PySpark Cassandra. They can be built with:

```bash
make dist
```

This creates 1) a fat jar with the Cassandra - Spark connector and additional classes for bridging Spark and PySpark for Cassandra data and 2) a python source distribution at:

* `target/pyspark_cassandra-<version>.jar`
* `target/pyspark_cassandra_<version>-<python version>.egg`.


Using with PySpark
------------------

```bash
spark-submit \
	--jars /path/to/pyspark_cassandra-<version>.jar \
	--jars --driver-class-path /path/to/pyspark_cassandra-<version>.jar \
	--py-files target/pyspark_cassandra_<version>-<python version>.egg \
	--conf spark.cassandra.connection.host=your,cassandra,node,names \
	--master spark://spark-master:7077 \
	yourscript.py
```


Using with PySpark shell
------------------------

Replace `spark-submit` with `pyspark` to start the interactive shell and don't provide a script as argument and then import PySpark Cassandra. Note that when performing this import the `sc` variable in pyspark is augmented with the `cassandraTable(...)` method. 

```python
import pyspark_cassandra
```


API
---

The PySpark Cassandra API aims to stay close to the Cassandra Spark Connector API. Reading its [documentation](https://github.com/datastax/spark-cassandra-connector/#documentation) is a good place to start.

### Data structures

PySpark Cassandra uses python dicts to represent CQL rows. CQL maps are supported through dicts. CQL sets and lists are supported through any iterable, but `set(...)` is obviously preferrable for CQL sets.


### CassandraSparkContext

A `CassandraSparkContext` is very similar to a regular `SparkContext`. It is created in the same way, can be used to read files, parallelize local data, broadcast a variable, etc. See the [Spark Programming Guide](https://spark.apache.org/docs/1.2.0/programming-guide.html) for more details. *But* it exposes one additional method:

* ``cassandraTable(keyspace, table)``:	Returns a CassandraRDD for the given keyspace and table.


### CassandraRDD

A `CassandraRDD` is equally very similar to a regular `RDD` in pyspark. 

* ``select(*columns)``: Creates a CassandraRDD with the select clause applied.
* ``where(clause, *args)``: Creates a CassandraRDD with a CQL where clause applied. The clause can contain ? markers with the arguments supplied as *args.
* ``saveToCassandra(...)``: See below.


### RDD's in general

PySpark Cassandra supports saving arbitrary RDD's to Cassandra using:

* ``saveToCassandra(keyspace, table, ...)``: Saves an RDD to Cassandra. The RDD is expected to contain dicts with keys mapping to CQL columns. Additional arguments which can be supplied are:

  * ``columns(iterable)``: The columns to save, i.e. which keys to take from the dicts in the RDD.
  * ``batch_size(int)``: The size in bytes to batch up in an unlogged batch of CQL inserts.
  * ``batch_buffer_size(int)``: The maximum number of batches which are 'pending'.
  * ``batch_level(string)``: The way batches are formed (defaults to "partition"):
     * ``all``: any row can be added to any batch
     * ``replicaset``: rows are batched for replica sets 
     * ``partition``: rows are batched by their partition key
  * ``consistency_level(cassandra.ConsistencyLevel)``: The consistency level used in writing to Cassandra.
  * ``parallelism_level(int)``: The maximum number of batches written in parallel.
  * ``ttl(int or timedelta)``: The time to live as milliseconds or timedelta to use for the values.
  * ``timestamp(int, date or datetime)``: The timestamp in milliseconds, date or datetime to use for the values.


Examples
--------

Creating a SparkContext with Cassandra support

```python
conf = SparkConf() \
	.setAppName("PySpark Cassandra Test") \
	.setMaster("spark://spark-master:7077") \
	.set("spark.cassandra.connection.host", "cas-1")
sc = CassandraSparkContext(conf=conf)
```

Using select and where to narrow the data in an RDD and then filter, map, reduce and collect it::

```python	
sc \
	.cassandraTable("keyspace", "table") \
	.select("col-a", "col-b") \
	.where("key=?", "x") \
	.filter(lambda r: r["col-b"].contains("foo")) \
	.map(lambda r: (r["col-a"], 1)
	.reduceByKey(lamba a, b: a + b)
	.collect()
```

Storing data in Cassandra::

```python
rdd = sc.parallelize([{
	"key": k,
	"stamp": datetime.now(),
	"val": random() * 10,
	"tags": ["a", "b", "c"],
	"options": {
		"foo": "bar",
		"baz": "qux",
	}
} for k in ["x", "y", "z"]])

rdd.saveToCassandra(
	"keyspace",
	"table",
	ttl=timedelta(hours=1),
)
```

