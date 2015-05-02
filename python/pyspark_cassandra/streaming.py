from pyspark.streaming.dstream import DStream
from pyspark_cassandra.conf import WriteConf
from pyspark_cassandra.rdd import RowFormat
from types import as_java_object, as_java_array


def saveToCassandra(dstream, keyspace, table, columns=None, write_conf=None, row_format=None):
    helper = dstream._sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass("pyspark_cassandra.PythonHelper").newInstance()
            
    write_conf = as_java_object(dstream._sc._gateway, write_conf.__dict__) if write_conf else None
    columns = as_java_array(dstream._sc._gateway, "String", columns) if columns else None

    helper.saveToCassandra(dstream._jdstream, keyspace, table, columns, write_conf, row_format)

# Monkey patch the default python DStream so that data in it can be stored to Cassandra as CQL rows
DStream.saveToCassandra = saveToCassandra
