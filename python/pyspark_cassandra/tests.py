from _functools import partial
from datetime import datetime, timedelta, tzinfo
from decimal import Decimal
import string
import sys
import time
import unittest
import uuid

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time

from pyspark import SparkConf
from pyspark_cassandra import CassandraSparkContext, RowFormat, Row, UDT
from pyspark_cassandra import streaming
import pyspark_cassandra
from pyspark_cassandra.conf import ReadConf, WriteConf
from pyspark.streaming.context import StreamingContext


class CassandraTestCase(unittest.TestCase):
    keyspace = "test_pyspark_cassandra"

    @classmethod
    def setUpClass(cls):
        super(CassandraTestCase, cls).setUpClass()
        cls.sc = CassandraSparkContext(conf=SparkConf().setAppName("PySpark Cassandra Test"))
        cls.session = Cluster().connect()
        cls.session.execute('''
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        ''' % (cls.keyspace,))
        cls.session.set_keyspace(cls.keyspace)

    @classmethod
    def tearDownClass(cls):
        super(CassandraTestCase, cls).tearDownClass()
        cls.session.shutdown()
        cls.sc.stop()

    def rdd(self, keyspace=None, table=None, key=None, column=None, **kwargs):
        keyspace = keyspace or getattr(self, 'keyspace', None)
        table = table or getattr(self, 'table', None)
        rdd = self.sc.cassandraTable(keyspace, table, **kwargs)
        if key is not None:
            rdd = rdd.where('key=?', key)
        if column is not None:
            rdd = rdd.select(column)
        return rdd

    def read_test(self, type_name, value=None):
        rdd = self.rdd(key=type_name, column=type_name)
        self.assertEqual(rdd.count(), 1)
        read = getattr(rdd.first(), type_name)
        self.assertEqual(read, value)
        return read

    def read_write_test(self, type_name, value):
        row = { 'key': type_name, type_name: value}
        rdd = self.sc.parallelize([row])
        rdd.saveToCassandra(self.keyspace, self.table)
        return self.read_test(type_name, value)



class SimpleTypesTestBase(CassandraTestCase):
    table = "simple_types"

    simple_types = [
        'ascii', 'bigint', 'blob', 'boolean', 'decimal', 'double', 'float',
        'inet', 'int', 'text', 'timestamp', 'timeuuid', 'varchar', 'varint',
        'uuid',
    ]

    @classmethod
    def setUpClass(cls):
        super(SimpleTypesTestBase, cls).setUpClass()
        cls.session.execute('''
            CREATE TABLE IF NOT EXISTS simple_types (
                key text primary key, %s
            )
        ''' % ', '.join('{0} {0}'.format(t) for t in cls.simple_types))

    def setUp(self):
        super(SimpleTypesTestBase, self).setUp()
        self.session.execute('TRUNCATE simple_types')


class SimpleTypesTest(SimpleTypesTestBase):
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
    # def test_inet(self):
    #    self.read_write_test('inet', u'/127.0.0.1')

    def test_int(self):
        self.read_write_test('int', 1)

    def test_text(self):
        self.read_write_test('text', u'some text')

    # TODO implement test with datetime with tzinfo without depending on pytz
    # def test_timestamp(self):
    #     self.read_write_test('timestamp', datetime(2015, 1, 1))

    def test_timeuuid(self):
        uuid = uuid_from_time(datetime(2015, 1, 1))
        self.read_write_test('timeuuid', uuid)

    def test_varchar(self):
        self.read_write_test('varchar', u'some varchar')

    def test_varint(self):
        self.read_write_test('varint', 1)

    def test_uuid(self):
        self.read_write_test('uuid', uuid.UUID('22dadfd0-b971-11e4-a856-85a08dca5bbf'))



class FormatTest(SimpleTypesTestBase):
    expected = Row(key='format-test', int=2, text='a')
    
    def setUp(self):
        super(FormatTest, self).setUp()
        self.sc.parallelize([self.expected]).saveToCassandra(self.keyspace, self.table)
        
    def read_as(self, row_format, keyed):
        table = self.rdd(row_format=row_format)
        if keyed:
            table = table.by_primary_key()
        table = table.where('key=?', self.expected.key)
        return table.first()
    
    def assert_rowtype(self, row_format, row_type, keyed=False):
        row = self.read_as(row_format, keyed)
        self.assertEqual(type(row), row_type)
        return row
    
    def assert_kvtype(self, row_format, kv_type):
        row = self.assert_rowtype(row_format, tuple, keyed=True)
        self.assertEqual(len(row), 2)
        k, v = row
        self.assertEqual(type(k), kv_type)
        self.assertEqual(type(v), kv_type)
        return k, v
        
    def test_tuple(self):
        row = self.assert_rowtype(RowFormat.TUPLE, tuple)
        self.assertEqual(self.expected.key, row[0])
        
    def test_kvtuple(self):
        k, _ = self.assert_kvtype(RowFormat.TUPLE, tuple)
        self.assertEqual(self.expected.key, k[0])
       
    def test_dict(self):
        row = self.assert_rowtype(RowFormat.DICT, dict)
        self.assertEqual(self.expected.key, row['key'])
      
    def test_kvdict(self):
        k, _ = self.assert_kvtype(RowFormat.DICT, dict)
        self.assertEqual(self.expected.key, k['key'])
      
    def test_row(self):
        row = self.assert_rowtype(RowFormat.ROW, pyspark_cassandra.Row)
        self.assertEqual(self.expected.key, row.key)
      
    def test_kvrow(self):
        k, _ = self.assert_kvtype(RowFormat.ROW, pyspark_cassandra.Row)
        self.assertEqual(self.expected.key, k.key)
        
        
        
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
        super(CollectionTypesTest, cls).tearDownClass()

    def setUp(self):
        super(CollectionTypesTest, self).setUp()
        self.session.execute('TRUNCATE %s' % self.table)

    def collections_common_tests(self, collection, column):
        rows = [
            {'key':k, column:v}
            for k, v in collection.items()
        ]
        
        self.sc.parallelize(rows).saveToCassandra(self.keyspace, self.table)
        
        rdd = self.sc.cassandraTable(self.keyspace, self.table).select('key', column).cache()
        self.assertEqual(len(collection), rdd.count())
        
        collected = rdd.collect()
        self.assertEqual(len(collection), len(collected))

        for row in collected:
            self.assertEqual(collection[row.key], getattr(row, column))
        
        return rdd
        
    def test_list(self):
        lists = {'l%s' % i: list(string.ascii_lowercase[:i]) for i in range(1, 10)}
        self.collections_common_tests(lists, 'l')
    
    def test_map(self):
        maps = {'m%s' % i : { k : 'x' for k in string.ascii_lowercase[:i] } for i in range(1, 10)}
        self.collections_common_tests(maps, 'm')
    
    def test_set(self):
        maps = {'s%s' % i : set(string.ascii_lowercase[:i]) for i in range(1, 10)}
        self.collections_common_tests(maps, 's')
            
            

class UDTTest(CassandraTestCase):
    table = "udt_types"
    
    types = {
        'simple_udt': {
            'col_text': 'text',
            'col_int': 'int',
            'col_boolean': 'boolean',
        },
        'udt_wset': {
            'col_text': 'text',
            'col_set': 'set<int>',
        },
    }

    @classmethod
    def setUpClass(cls):
        super(UDTTest, cls).setUpClass()
        
        for name, udt in cls.types.items():
            cls.session.execute('''
                CREATE TYPE IF NOT EXISTS %s (
                    %s
                )
            ''' % (name, ',\n\t'.join('%s %s' % (field, ftype) for field, ftype in udt.items())))
        
        cls.session.execute('''
            CREATE TABLE IF NOT EXISTS %s (
                key text primary key, %s
            )
        ''' % (cls.table, ', '.join('%s frozen<%s>' % (name, name) for name in cls.types)))

    @classmethod
    def tearDownClass(cls):
        super(UDTTest, cls).tearDownClass()

    def setUp(self):
        super(UDTTest, self).setUp()
        self.session.execute('TRUNCATE %s' % self.table)

    def read_write_test(self, type_name, value):
        read = super(UDTTest, self).read_write_test(type_name, value)
        self.assertTrue(isinstance(read, UDT), 'value read is not an instance of UDT')
        
        udt = self.types[type_name]
        if udt:
            for field in udt:
                self.assertEqual(getattr(read, field), value[field])
    
    def test_simple_udt(self):
        self.read_write_test('simple_udt', UDT(col_text='text', col_int=1, col_boolean=True))
    
    def test_simple_udt_null(self):
        self.read_write_test('simple_udt', UDT(col_text='text', col_int=None, col_boolean=True))
    
    def test_udt_wset(self):
        self.read_write_test('udt_wset', UDT(col_text='text', col_set={1, 2, 3}))
            
            

class JoinTest(SimpleTypesTestBase):
    records = {
       str(c) : str(i) for i, c in
       enumerate(string.ascii_lowercase)
    }
    
    def setUp(self):
        super(JoinTest, self).setUp()
        self.session.execute('TRUNCATE %s' % self.table)
        
        for k, v in self.records.items():
            self.session.execute('INSERT INTO ' + self.table + ' (key, text) values (%s, %s)', (k, v))

    def test_join_with_cassandra(self):
        rdd = self.sc.parallelize(self.records.items())
        self.assertEqual(dict(rdd.collect()), self.records)
        
        joined = rdd.joinWithCassandraTable(self.keyspace, self.table).on('key').select('key', 'text').cache()
        self.assertEqual(dict(joined.keys().collect()), dict(joined.values().collect()))
        for (k, v) in joined.collect():
            self.assertEqual(k, v)
        
        # TODO test
        # .where()
        # .limit()


class ConfTest(SimpleTypesTestBase):
    # TODO this is still a very basic test, more cases and (better) validation required
    def setUp(self):
        super(SimpleTypesTestBase, self).setUp()
        for i in range(100):
            self.session.execute("INSERT INTO %s (key, text, int) values ('%s', '%s', %s)" % (self.table, i, i, i))
    
    def test_read_conf(self):
        self.rdd(split_count=100).collect()
        self.rdd(split_size=32).collect()
        self.rdd(fetch_size=100).collect()
        self.rdd(consistency_level='LOCAL_QUORUM').collect()
        self.rdd(consistency_level=ConsistencyLevel.LOCAL_QUORUM).collect()
        self.rdd(metrics_enabled=True).collect()
        self.rdd(read_conf=ReadConf(split_count=10, consistency_level='ALL', metrics_enabled=True)).collect()
        
    def test_write_conf(self):
        rdd = self.sc.parallelize([{'key':i, 'text':i, 'int':i} for i in range(10)])
        save = partial(rdd.saveToCassandra, self.keyspace, self.table)
        
        save(batch_size=100)
        save(batch_buffer_size=100)
        save(batch_grouping_key='replica_set')
        save(batch_grouping_key='partition')
        save(consistency_level='ALL')
        save(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        save(parallelism_level=10)
        save(throughput_mibps=10)
        save(ttl=5)
        save(ttl=timedelta(minutes=30))
        save(timestamp=time.clock() * 1000 * 1000)
        save(timestamp=datetime.now())
        save(metrics_enabled=True)
        save(write_conf=WriteConf(ttl=3, metrics_enabled=True))
    
    
class StreamingTest(SimpleTypesTestBase):
    @classmethod
    def setUpClass(cls):
        super(StreamingTest, cls).setUpClass()
        cls.ssc = StreamingContext(cls.sc, 1)
    
    def test_streaming(self):
        size = 10
        count = 3
        
        rows = [ 
            [
                {'key': str(j * size + i), 'text': str(j * size + i)}
                for i in range(size)
            ]
            for j in range(count)
        ]
        
        rdds = list(map(self.sc.parallelize, rows))        
        self.ssc.queueStream(rdds).saveToCassandra(self.keyspace, self.table)
        
        self.ssc.start()
        self.ssc.awaitTermination(count + 1)
        self.ssc.stop(stopSparkContext=False, stopGraceFully=True)
        
        read = self.rdd(row_format=RowFormat.TUPLE).select('key', 'text').by_primary_key().collect()
        self.assertEqual(len(read), size * count)
        for (k, v) in read:
            self.assertEqual(k, v)

        
class RegressionTest(CassandraTestCase):
    def test_64(self):
        self.session.execute('''
            CREATE TABLE IF NOT EXISTS test_64 (
                delay double PRIMARY KEY,
                pdf list<double>,
                pos list<double>
            )
        ''')
        self.session.execute('''TRUNCATE test_64''')
        
        res = ([0.0, 1.0, 2.0], [12.0, 3.0, 0.0], 0.0)
        rdd = self.sc.parallelize([res])
        rdd.saveToCassandra(self.keyspace, 'test_64', columns=['pos', 'pdf', 'delay'])
        
        row = self.rdd(table='test_64').first()
        self.assertEqual(row.pos, res[0])
        self.assertEqual(row.pdf, res[1])
        self.assertEqual(row.delay, res[2])


if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase(StreamingTest)
    unittest.TextTestRunner().run(suite)
    
