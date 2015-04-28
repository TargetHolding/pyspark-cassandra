from datetime import datetime
from decimal import Decimal
import string
import sys
import unittest
import uuid

from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time

from pyspark import SparkConf
from pyspark_cassandra import CassandraSparkContext, RowFormat, Row, UDT


class CassandraTestCase(unittest.TestCase):
    keyspace = "test_pyspark_cassandra"

    @classmethod
    def setUpClass(cls):
        super(CassandraTestCase, cls).setUpClass()
        cls.sc = CassandraSparkContext(conf=SparkConf().setAppName("PySpark Cassandra Test"))
        cls.session = Cluster().connect()
        cls.session.execute('''
            CREATE KEYSPACE IF NOT EXISTS test_pyspark_cassandra
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        ''')
        cls.session.set_keyspace('test_pyspark_cassandra')


    @classmethod
    def tearDownClass(cls):
        super(CassandraTestCase, cls).tearDownClass()
        cls.session.shutdown()
        cls.sc.stop()


class SimpleTypesTest(CassandraTestCase):
    table = "simple_types"

    simple_types = [
        'ascii', 'bigint', 'blob', 'boolean', 'decimal', 'double', 'float',
        'inet', 'int', 'text', 'timestamp', 'timeuuid', 'varchar', 'varint',
        'uuid',
    ]

    @classmethod
    def setUpClass(cls):
        super(SimpleTypesTest, cls).setUpClass()
        cls.session.execute('''
            CREATE TABLE IF NOT EXISTS simple_types (
                key text primary key, %s
            )
        ''' % ', '.join('{0} {0}'.format(t) for t in cls.simple_types))

    @classmethod
    def tearDownClass(cls):
        cls.session.execute('DROP TABLE simple_types')
        super(SimpleTypesTest, cls).tearDownClass()

    def setUp(self):
        super(SimpleTypesTest, self).setUp()
        self.session.execute('TRUNCATE simple_types')


    def simple_type_rdd(self, type_name):
        return (
            self.sc
                .cassandraTable(self.keyspace, self.table)
                .where('key=?', type_name).select(type_name)
        )

    def read_test(self, type_name, value=None):
        rdd = self.simple_type_rdd(type_name)
        self.assertEqual(rdd.count(), 1)
        self.assertEqual(rdd.first().__getattr__(type_name), value)
        return rdd

    def read_write_test(self, type_name, value):
        self.sc.parallelize([{ 'key': type_name, type_name: value}]).saveToCassandra(self.keyspace, self.table)
        self.read_test(type_name, value)

    def test_ascii(self):
        self.read_write_test('ascii', 'some ascii')

    def test_bigint(self):
        self.read_write_test('bigint', sys.maxint)

    def test_blob(self):
        self.read_write_test('blob', bytearray('some blob'))

    def test_boolean(self):
        self.read_write_test('boolean', False)

    def test_decimal(self):
        self.read_write_test('decimal', Decimal(0.5))

    def test_double(self):
        self.read_write_test('double', 0.5)

    def test_float(self):
        self.read_write_test('float', 0.5)

    # TODO returns resolved hostname with ip address (hostname/ip,
    # e.g. /127.0.0.1), but doesn't accept with / ...
    #def test_inet(self):
    #    self.read_write_test('inet', u'/127.0.0.1')

    def test_int(self):
        self.read_write_test('int', 1)

    def test_text(self):
        self.read_write_test('text', u'some text')

    def test_timestamp(self):
        self.read_write_test('timestamp', datetime(2015, 1, 1))

    # Invalid version for TimeUUID type.
    def test_timeuuid(self):
        uuid = uuid_from_time(datetime(2015, 1, 1))
        self.read_write_test('timeuuid', uuid)

    def test_varchar(self):
        self.read_write_test('varchar', u'some varchar')

    def test_varint(self):
        self.read_write_test('varint', 1)

    def test_uuid(self):
        self.read_write_test('uuid', uuid.UUID('22dadfd0-b971-11e4-a856-85a08dca5bbf'))


class CollectionTypesTest(CassandraTestCase):
    table = "collection_types"
    collection_types = {
        'm': 'map<text, text>',
        'l': 'list<text>',
        's': 'set<text>',
    }

    @classmethod
    def setUpClass(cls):
        super(CollectionTypesTest, cls).setUpClass()
        cls.session.execute('''
            CREATE TABLE IF NOT EXISTS %s (
                key text primary key, %s
            )
        ''' % (cls.table, ', '.join('%s %s' % (k, v) for k, v in cls.collection_types.items())))

    @classmethod
    def tearDownClass(cls):
        # cls.session.execute('DROP TABLE %s' % cls.table)
        super(CollectionTypesTest, cls).tearDownClass()

    def setUp(self):
        super(CollectionTypesTest, self).setUp()
        self.session.execute('TRUNCATE %s' % self.table)

    def test_map(self):
        maps = [
            {
                'key': 'm%s' % i,
                'm': { k : 'x' for k in string.ascii_lowercase[:i] }
            } for i in range(1, 10)
        ]

        maps_by_key = {
            m['key'] : m['m']
            for m in maps
        }

        # requires no. partitions to equal the no. elements in maps
        # PickleRowWriter.unpickle(...) doesn't accept batched unpickling just yet
        self.sc.parallelize(maps, len(maps)).saveToCassandra(self.keyspace, self.table)

        rdd = self.sc.cassandraTable(self.keyspace, self.table).select('key', 'm').cache()
        self.assertEqual(len(maps), rdd.count())

        collected = rdd.collect()
        self.assertEqual(len(maps), len(collected))

        for row in collected:
            self.assertEqual(maps_by_key[row.key], row.m)

if __name__ == '__main__':
    unittest.main()

# session.execute('''
#     CREATE TABLE IF NOT EXISTS counter (
#         key text primary key,
#         counter counter,
#     )
# ''')
#
# session.execute('''
#     CREATE TYPE IF NOT EXISTS test_udt (
#         text text,
#         int int
#     )
# ''')
#
# session.execute('''
#     CREATE TABLE IF NOT EXISTS udt_type (
#         key text primary key,
#         udt frozen<test_udt>
#     )
# ''')
#
