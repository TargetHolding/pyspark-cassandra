import sys

from py4j.java_gateway import java_import

from pyspark.conf import SparkConf
from pyspark import SparkContext


def main():
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: example <keyspace_name> <column_family_name>"
        sys.exit(-1)

    keyspace_name = sys.argv[1]
    column_family_name = sys.argv[2]

    # Valid config options here https://github.com/datastax/spark-cassandra-connector/blob/master/doc/1_connecting.md
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")

    sc = SparkContext(appName="Spark + Cassandra Example",
                      conf=conf)

    # import time; time.sleep(30)
    java_import(sc._gateway.jvm, "com.datastax.spark.connector.CassandraJavaUtil")
    print sc._jvm.CassandraJavaUtil

    users = (
        ["Mike", "Sukmanowsky"],
        ["Andrew", "Montalenti"],
        ["Keith", "Bourgoin"],
    )
    rdd = sc.parallelize(users)
    print rdd.collect()


if __name__ == '__main__':
    main()