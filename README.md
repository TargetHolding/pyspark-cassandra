PySpark Cassandra
=================

PySpark Cassandra brings back the fun in working with Cassandra data in PySpark.

This module provides python support for Apache Spark's Resillient Distributed Datasets from Apache Cassandra CQL rows using [Cassandra Spark Connector](https://github.com/datastax/spark-cassandra-connector) within PySpark, both in the interactive shell and in python programmes submitted with spark-submit.

This project was initially forked from https://github.com/Parsely/pyspark-cassandra, but in order to submit it to http://spark-packages.org/, a plain old repository was created. 

**Contents:**
* [Compatibility](#compatibility)
* [Building](#building)
* [Using with PySpark](#using-with-pyspark)
* [Using with PySpark shell](#using-with-pyspark-shell)
* [API](#api)
* [Examples](#examples)
* [Problems / ideas?](#problems--ideas)
* [Contributing](#contributing)



Compatibility
-------------

Currently PySpark Cassandra has been succesfully used with Spark version 1.2.0 and 1.2.1 and DataStax Spark Cassandra Connector version 1.2. Feedback on (in-)compatibility with other versions is much appreciated.



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


### pyspark_cassandra.RowFormat

The primary representation of CQL rows in PySpark Cassandra is the ROW format. However `sc.cassandraTable(...)` supports the `row_format` argument which can be any of the constants from `RowFormat`:
* `DICT`: The default layout, a CQL row is represented as a python dict with the CQL row columns as keys.
* `TUPLE`: A CQL row is represented as a python tuple with the values in CQL table column order / the order of the selected columns.
* `KV_DICTS`: A tuple of two python dicts represents the primary key columns and remaining (value) columns respectively.
* `KV_TUPLES`: A tuple of two python tuples represents the primary key columns and remaining (value) columns respectively. The values in the tuple are in the CQL table column order / the order of the selected columns.
* `ROW`: A pyspark_cassandra.Row object representing a CQL row.

Column values are related between CQL and python as follows:

|  **CQL**  |       **python**      |
|:---------:|:---------------------:|
|   ascii   |    unicode string     |
|   bigint  |         long          |
|    blob   |       bytearray       |
|  boolean  |        boolean        |
|  counter  |       int, long       |
|  decimal  |        decimal        |
|   double  |         float         |
|   float   |         float         |
|    inet   |          str          |
|    int    |          int          |
|    map    |         dict          |
|    set    |          set          |
|    list   |         list          |
|    text   |    unicode string     |
| timestamp |   datetime.datetime   |
|  timeuuid |       uuid.UUID       |
|  varchar  |    unicode string     |
|   varint  |         long          |
|    uuid   |       uuid.UUID       |
|   _UDT_   | pyspark_cassandra.UDT |


### pyspark_cassandra.Row

This is the default type to which CQL rows are mapped. It is directly compatible with `pyspark.sql.Row` but is (correctly) mutable and provides some other improvements.


### pyspark_cassandra.UDT

This type is structurally identical to pyspark_cassandra.Row but serves user defined types. Mapping to custom python types (e.g. via CQLEngine) is not yet supported.
 

### pyspark_cassandra.CassandraSparkContext

A `CassandraSparkContext` is very similar to a regular `SparkContext`. It is created in the same way, can be used to read files, parallelize local data, broadcast a variable, etc. See the [Spark Programming Guide](https://spark.apache.org/docs/1.2.0/programming-guide.html) for more details. *But* it exposes one additional method:

* ``cassandraTable(keyspace, table, ...)``:	Returns a CassandraRDD for the given keyspace and table. Additional arguments which can be provided:

  * `row_format` can be set to any of the `pyspark_cassandra.RowFormat` values (defaults to `ROW`)
  * `split_size` sets the size in the number of CQL rows in each partition (defaults to `100000`)
  * `fetch_size` sets the number of rows to fetch per request from Cassandra (defaults to `1000`)
  * `consistency_level` sets with which consistency level to read the data (defaults to `LOCAL_ONE`)


### pyspark.RDD

PySpark Cassandra supports saving arbitrary RDD's to Cassandra using:

* ``rdd.saveToCassandra(keyspace, table, ...)``: Saves an RDD to Cassandra. The RDD is expected to contain dicts with keys mapping to CQL columns. Additional arguments which can be supplied are:

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


### pyspark_cassandra.CassandraRDD

A `CassandraRDD` is very similar to a regular `RDD` in pyspark. It is extended with the following methods: 

* ``select(*columns)``: Creates a CassandraRDD with the select clause applied.
* ``where(clause, *args)``: Creates a CassandraRDD with a CQL where clause applied. The clause can contain ? markers with the arguments supplied as *args.
* ``saveToCassandra(...)``: As above, but the keyspace and/or table __may__ be omitted to save to the same keyspace and/or table. 



Examples
--------

Creating a SparkContext with Cassandra support

```python
import pyspark_cassandra

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


Problems / ideas?
-----------------
Feel free to use the issue tracker propose new functionality and / or report bugs.



Contributing
------------

1. Fork it
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create new Pull Request
