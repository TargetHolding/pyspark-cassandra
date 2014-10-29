# WARNING: Work in progress
from pyspark.context import SparkContext
from pyspark.serializers import BatchedSerializer, PickleSerializer
from pyspark.rdd import RDD

from py4j.java_gateway import java_import


class CassandraSparkContext(SparkContext):

    def _do_init(self, *args, **kwargs):
        # Modifies base _do_init to add a Java-Cassandra SparkContext (jcsc)
        # to the instance
        super(CassandraSparkContext, self)._do_init(*args, **kwargs)
        java_import(self._jvm, "com.datastax.spark.connector.CassandraJavaUtil")
        java_import(self._jvm, "com.datastax.spark.connector.RowConvertingIterator")
        self._jcsc = self._jvm.CassandraJavaUtil.javaFunctions(self._jsc)

    def cassandraTable(self, keyspace, table):
        """Returns all the Rows in a Cassandra keyspace and table as an RDD.

        @param keyspace: Cassandra keyspace / schema name
        @param table: Cassandra table / column family name
        """
        # Unsure right now if we need CassandraSerializer, but likely do since
        # we'll get generic CassandraRow instances back that we'll need to
        # inspect?
        # return RDD(self._jcsc.cassandraTable(keyspace, table), self,
        #            CassandraSerializer())
        return RDD(self._jcsc.cassandraTable(keyspace, table),
                   self, BatchedSerializer(PickleSerializer()))


# Unfortunately, can't call rdd.saveToCassandra as we'd dynamically have to
# bind a method to all rdd instances which isn't feasible
def saveToCassandra(rdd, keyspace, table):
    pickledRDD = rdd._toPickleSerialization()
    rdd.ctx._jvm.CassandraJavaUtil.javaFunctions(pickledRDD._jrdd)\
        .saveToCassandra(keyspace, table)
