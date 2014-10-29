"""
Usage:
    pyspark_cassandra_hadoop_example.py (init|run) <keyspace>

Arguments:
    <command>   One of "init" or "run"
    <keyspace>  The name of the keyspace where example data is stored
"""
import datetime as dt
import json
import sys

from pyspark import SparkContext


INPUT_KEY_CONVERTER = "com.parsely.spark.converters.FromUsersCQLKeyConverter"
INPUT_VALUE_CONVERTER = "com.parsely.spark.converters.FromUsersCQLValueConverter"
OUTPUT_KEY_CONVERTER = "com.parsely.spark.converters.ToCassandraCQLKeyConverter"
OUTPUT_VALUE_CONVERTER = "com.parsely.spark.converters.ToCassandraCQLValueConverter"


def create_keyspace_if_not_exists(cassandra_session, keyspace,
                                  replication_options=None):
    replication_options = replication_options or \
                          "{'class': 'SimpleStrategy', 'replication_factor': 1}"
    rows = cassandra_session.execute("SELECT keyspace_name FROM system.schema_keyspaces;")
    if not any((row.keyspace_name == keyspace for row in rows)):
        cassandra_session.execute("""
            CREATE SCHEMA {}
            WITH REPLICATION={};
        """.format(keyspace, replication_options))
        return True
    else:
        return False


def create_schemas(keyspace):
    """Utility function to create keyspaces and tables for example.

    Requires taht you have the Python Cassandra driver installed on your
    PYTHONPATH http://datastax.github.io/python-driver/installation.html.
    """
    from cassandra.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()

    create_keyspace_if_not_exists(session, keyspace)
    session.set_keyspace(keyspace)

    session.execute("""
        CREATE TABLE IF NOT EXISTS {}.users (
            user_id text PRIMARY KEY,
            created_at timestamp,
            updated_at timestamp,
            first_name text,
            last_name text,
            emails set<text>,
            logins list<text>,
            settings map<text, text>
        );
    """.format(keyspace))

    print "Keyspace {!r} and table {!r} created.".format(keyspace, "users")

    stmt = session.prepare("""
        INSERT INTO {}.users (user_id, created_at, updated_at, first_name,
                              last_name, emails, logins, settings)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """.strip().format(keyspace))

    now = dt.datetime.now()
    session.execute(stmt.bind(("mike", now, now, "Mike", "Sukmanowsky",
                    set(["mike@parsely.com"]), [now.isoformat()],
                    {"background_color": "red"})))
    session.execute(stmt.bind(("andrew", now, now, "Andrew", "Montalenti",
                    set(["andrew@parsely.com"]), [now.isoformat()],
                    {"background_color": "black"})))

    print "Inserted 2 sample users."


def to_cql_output_format(user):
    return (
        # partition and clustering keys
        {"user_id": user["id"]},
        # columns in update CQL
        [
            user["created_at"],
            user["updated_at"],
            user["first_name"],
            user["last_name"],
            user["emails"],
            user["logins"],
            user["settings"],
        ]
    )


def run_driver(keyspace):
    sc = SparkContext(appName="PySpark Cassandra Hadoop Example")

    # Reading from Cassandra
    conf = {
        "cassandra.input.thrift.address": "localhost",
        "cassandra.input.thrift.port": "9160",
        "cassandra.input.keyspace": keyspace,
        "cassandra.input.columnfamily": "users",
        "cassandra.input.partitioner.class":"Murmur3Partitioner",
        "cassandra.input.page.row.size": "5000"
    }
    cass_rdd = sc.newAPIHadoopRDD(
        # inputFormatClass
        "org.apache.cassandra.hadoop.cql3.CqlInputFormat",
        # keyClass
        "java.util.Map",
        # valueClass
        "java.util.Map",
        keyConverter=INPUT_KEY_CONVERTER,
        valueConverter=INPUT_VALUE_CONVERTER,
        conf=conf)
    print cass_rdd.collect()

    # Writing to Cassandra
    now = dt.datetime.now()
    users = (
        {
            "id": "keith",
            "created_at": now,
            "updated_at": now,
            "first_name": "Keith",
            "last_name": "Bourgoin",
            "emails": set(["keith@parsely.com"]),
            "logins": [now.isoformat()],
            "settings": {
                "background_color": "blue",
            },
        },
        {
            "id": "toms",
            "created_at": now,
            "updated_at": now,
            "first_name": "Toms",
            "last_name": "Baugis",
            "emails": set(["toms@parsely.com"]),
            "logins": [now.isoformat()],
            "settings": {
                "background_color": "green",
            },
        },
    )

    cql = """
        UPDATE users
        SET created_at=?, updated_at=?, first_name=?, last_name=?, emails=?,
            logins=?, settings=?
    """.strip()
    conf = {
        "cassandra.output.thrift.address": "localhost",
        "cassandra.output.thrift.port": "9160",
        "cassandra.output.keyspace": keyspace,
        "cassandra.output.partitioner.class": "Murmur3Partitioner",
        "cassandra.output.cql": cql,
        "mapreduce.output.basename": "users",
        "mapreduce.outputformat.class": "org.apache.cassandra.hadoop.cql3.CqlOutputFormat",
        "mapreduce.job.output.key.class": "java.util.Map",
        "mapreduce.job.output.value.class": "java.util.List"
    }
    users = sc.parallelize(users)
    users.map(to_cql_output_format)\
         .saveAsNewAPIHadoopDataset(conf=conf,
                                    keyConverter=OUTPUT_KEY_CONVERTER,
                                    valueConverter=OUTPUT_VALUE_CONVERTER)

    sc.stop()


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] not in ("init", "run"):
        sys.stderr.write(__doc__)
        sys.exit(-1)

    command = sys.argv[1]
    keyspace = sys.argv[2]

    if command == "init":
        create_schemas(keyspace)
    else:
        run_driver(keyspace)

    print "Done."
