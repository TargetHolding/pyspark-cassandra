"""
WARNING: This example is a work in progress!

Usage:
    pyspark_cassandra_example.py (init|run) <keyspace> <table>

Arguments:
    <command>   One of "init" or "run"
    <keyspace>  The name of the keyspace where pixel data is stored
    <table>     The name of the table where pixel data is stored
"""
import datetime as dt
import sys
from uuid import uuid4

from pyspark.context import SparkConf
from pyspark_cassandra import CassandraSparkContext, saveToCassandra


def create_schemas(keyspace, table):
    """Utility function to create schemas and tables for example.

    Requires taht you have the Python Cassandra driver installed on your
    PYTHONPATH http://datastax.github.io/python-driver/installation.html.
    """
    from cassandra.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect(keyspace)

    # Check to see if the schema/keyspace already exists
    rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces;")
    if not any((row.keyspace_name == keyspace for row in rows)):
        session.execute("""
            CREATE SCHEMA {}
            WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1};
        """.format(keyspace))
        print "Created keyspace: {!r}".format(keyspace)
    else:
        print "Keyspace {!r} exists, skipping creation.".format(keyspace)

    session.execute("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            customer_id text,
            url text,
            hour timestamp,
            ts timestamp,
            pixel_id text,
            data map<text, text>,
            PRIMARY KEY ((customer_id, url, hour), ts, pixel_id)
        );
    """.format(keyspace, table))
    print "Created table: {!r}.{!r}".format(keyspace, table)

    stmt = session.prepare("""
        UPDATE {}.{} SET data=? WHERE customer_id=? AND url=? AND hour=?
            AND ts=? AND pixel_id=?;
    """.strip().format(keyspace, table))

    pixels = (
        ({"visitor_id": "1234"},  # data
         "example.com",  # customer_id
         "http://example.com/",  # url
         dt.datetime(2014, 1, 1, 1),  # hour
         dt.datetime(2014, 1, 1, 1, 23, 12),  # ts
         "8620d3a2-8e03-4f03-bf96-d97369a4c3dc"),  # pixel_id
        ({"visitor_id": "1234"}, "example.com", "http://example.com/",
         dt.datetime(2014, 1, 1, 1), dt.datetime(2014, 1, 1, 1, 23, 22),
         "9cab5264-d192-4e0e-ab32-84ebc07d7ed9"),
        ({"visitor_id": "1234"}, "example.com", "http://example.com/",
         dt.datetime(2014, 1, 1, 1), dt.datetime(2014, 1, 1, 1, 25, 22),
         "cb6f1a9e-77d6-4868-a336-c0d736d10d84"),
        ({"visitor_id": "abcd"}, "example.com", "http://example.com/",
         dt.datetime(2014, 1, 1, 1), dt.datetime(2014, 1, 1, 1, 25, 22),
         "c82b1655-1408-4072-b53c-7fd923e8a0c8"),
    )
    for pixel in pixels:
        session.execute(stmt.bind(pixel))

    print "Inserted sample data into: {!r}.{!r}".format(keyspace, table)


def run_driver(keyspace, table):
    conf = SparkConf().setAppName("PySpark Cassandra Sample Driver")
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    sc = CassandraSparkContext(conf=conf)

    # Read some data from Cassandra
    pixels = sc.cassandraTable(keyspace, table)
    print pixels.first()

    # Count unique visitors, notice that the data returned by Cassandra is
    # a dict-like, you can access partition, clustering keys as well as
    # columns by name. CQL collections: lists, sets and maps are converted
    # to proper Python data types
    visitors = pixels.map(lambda p: (p["data"]["visitor_id"],))\
                .distinct()
    print "Visitors: {:,}".format(visitors.count())

    # Insert some new pixels into the table
    pixels = (
        {
            "customer_id": "example.com",
            "url": "http://example.com/article1/",
            "hour": dt.datetime(2014, 1, 2, 1),
            "ts": dt.datetime(2014, 1, 2, 1, 8, 23),
            "pixel_id": str(uuid4()),
            "data": {"visitor_id": "xyz"}
        },
    )
    saveToCassandra(sc.parallelize(pixels), keyspace, table)
    print "Wrote new pixels to Cassandra {!r}.{!r}".format(keyspace, table)


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in ("init", "run"):
        sys.stderr.write(__doc__)
        sys.exit(-1)

    command = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]

    if command == "init":
        create_schemas(keyspace, table)
    else:
        run_driver(keyspace, table)

    print "Done."



if __name__ == '__main__':
    main()