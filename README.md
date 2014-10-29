# pyspark-cassandra

Utilities and examples to asssist in working with Cassandra and PySpark.

Currently contains an updated and much more robust example of using a
SparkContext's [`newAPIHadoopRDD`](https://spark.apache.org/docs/1.1.0/api/python/pyspark.context.SparkContext-class.html#newAPIHadoopRDD)
to read from and an RDD's [`saveAsNewAPIHadoopDataset`](https://spark.apache.org/docs/1.1.0/api/python/pyspark.rdd.RDD-class.html#saveAsNewAPIHadoopDataset)
to write to Cassandra 2.1. Demonstrates usage of CQL collections:
[lists](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_list_t.html),
[sets](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_set_t.html) and
[maps](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_map_t.html).

Working on proper integration with the DataStax Cassandra Spark Connector.

## Building

You'll need Maven in order to build the uberjar required for the examples.

```bash
mvn clean package
```

Will create an uberjar at `target/pyspark-cassandra-<version>-SNAPSHOT.jar`.

## Using with PySpark

```bash
spark-submit --driver-class-path /path/to/pyspark-cassandra.jar myscript.py ...
```

## Using examples

```bash
pip install -r requirments.txt
```

Then run examples either directly with `spark-submit`, or use the
`run_script.py` utility.

### Running the PySpark Cassandra Hadoop Example

The example can first create the schema it requires via:

```bash
./run_script.py src/main/python/pyspark_cassandra_hadoop_example init test
```

The init command initializes the keyspace, table and inserts sample data.
`"test"` is the name of the keyspace. A users table will be created in
this keyspace with two sample users to enable reading.

Afterwards, you can run:

```bash
./run_script src/main/python/pyspark_cassandra_hadoop_example run test
```

Which runs a sample PySpark driver program that reads the existing values in
the `users` table and then writes two new users to this table.
