from py4j.java_gateway import java_import
from pyspark.streaming.dstream import DStream
from pyspark_cassandra.conf import WriteConf
from pyspark_cassandra.rdd import RowFormat


def saveToCassandra(dstream, keyspace, table, columns=None, write_conf=None, row_format=None):
    jvm = dstream._sc._jvm
    
    try:
        jvm.Class.forName("pyspark_cassandra.RowFormat")
        java_import(jvm, "com.datastax.spark.connector.japi.*")
        java_import(jvm, "pyspark_cassandra.*")
        java_import(jvm, "pyspark_cassandra.pickling.*")
    except Exception as e:
        raise ImportError("Java module pyspark_cassandra not found (%s)" % e)
    
    row_format = jvm.RowFormat.values()[row_format] if row_format else None
    writer_factory = jvm.ObjectRowWriterFactory(row_format)

    builder = jvm.CassandraStreamingJavaUtil.javaFunctions(
                  dstream._jdstream.flatMap(jvm.BatchUnpickle())
              ).writerBuilder(keyspace, table, writer_factory)
    
    # TODO should be optional, but we need the default because metrics_enabled is set to False in pyspark_cassandra
    # if write_conf: builder = builder.withWriteConf(write_conf.to_java_conf())
    builder = builder.withWriteConf((write_conf or WriteConf(dstream._sc)).to_java_conf())

    if columns:
        columns = jvm.CassandraJavaUtil.someColumns(str(c) for c in columns)
        builder = builder.withColumnSelector(columns)

    builder.saveToCassandra()

# Monkey patch the default python DStream so that data in it can be stored to Cassandra as CQL rows
DStream.saveToCassandra = saveToCassandra
