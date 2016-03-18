# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from _functools import partial
from datetime import datetime, timedelta
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
from pyspark.accumulators import AddingAccumulatorParam
from pyspark.streaming.context import StreamingContext

from pyspark_cassandra import CassandraSparkContext, RowFormat, Row, UDT
import pyspark_cassandra
import pyspark_cassandra.streaming
from pyspark_cassandra.conf import ReadConf, WriteConf
from itertools import chain
from math import sqrt
from uuid import UUID


class CassandraTestCase(unittest.TestCase):
    keyspace = "test_pyspark_cassandra"

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
        row = {'key': type_name, type_name: value}
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
            CREATE TABLE IF NOT EXISTS ''' + cls.table + ''' (
                key text primary key, %s
            )
        ''' % ', '.join('{0} {0}'.format(t) for t in cls.simple_types))

    def setUp(self):
        super(SimpleTypesTestBase, self).setUp()
        self.session.execute('TRUNCATE ' + self.table)


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
        maps = {'m%s' % i : {k : 'x' for k in string.ascii_lowercase[:i]} for i in range(1, 10)}
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

        cls.udt_support = cls.session.cluster.protocol_version >= 4
        if cls.udt_support:
            for name, udt in cls.types.items():
                cls.session.execute('''
                    CREATE TYPE IF NOT EXISTS %s (
                        %s
                    )
                ''' % (name, ',\n\t'.join('%s %s' % f for f in udt.items())))

            fields = ', '.join(
                '{udt_type} frozen<{udt_type}>'.format(udt_type=udt_type)
                for udt_type in cls.types
            )

            fields += ', ' + ', '.join(
                '{udt_type}_{col_type} {col_type}<frozen<{udt_type}>>'.format(udt_type=udt_type, col_type=col_type)
                for udt_type in cls.types
                for col_type in ('set', 'list')
            )

            cls.session.execute('''
                CREATE TABLE IF NOT EXISTS %s (
                    key text primary key, %s
                )
            ''' % (cls.table, fields))

    def setUp(self):
        if not self.udt_support:
            self.skipTest("testing with Cassandra < 2.2, can't test with UDT's")

        super(UDTTest, self).setUp()
        self.session.execute('TRUNCATE %s' % self.table)

    def read_write_test(self, type_name, value):
        read = super(UDTTest, self).read_write_test(type_name, value)
        self.assertTrue(isinstance(read, UDT),
                        'value read is not an instance of UDT')

        udt = self.types[type_name]
        for field in udt:
            self.assertEqual(getattr(read, field), value[field])

    def test_simple_udt(self):
        self.read_write_test('simple_udt', UDT(col_text='text', col_int=1, col_boolean=True))

    def test_simple_udt_null(self):
        super(UDTTest, self).read_write_test('simple_udt', None)

    def test_simple_udt_null_field(self):
        self.read_write_test('simple_udt', UDT(col_text='text', col_int=None, col_boolean=True))
        self.read_write_test('simple_udt', UDT(col_text=None, col_int=1, col_boolean=True))

    def test_udt_wset(self):
        self.read_write_test('udt_wset', UDT(col_text='text', col_set={1, 2, 3}))

    def test_collection_of_udts(self):
        super(UDTTest, self).read_write_test('simple_udt_list', None)

        udts = [UDT(col_text='text ' + str(i), col_int=i, col_boolean=bool(i % 2)) for i in range(10)]
        super(UDTTest, self).read_write_test('simple_udt_set', set(udts))
        super(UDTTest, self).read_write_test('simple_udt_list', udts)

        udts = [UDT(col_text='text ' + str(i), col_int=i, col_boolean=None) for i in range(10)]
        super(UDTTest, self).read_write_test('simple_udt_set', set(udts))
        super(UDTTest, self).read_write_test('simple_udt_list', udts)



class SelectiveSaveTest(SimpleTypesTestBase):
    def _save_and_get(self, *row):
        columns = ['key', 'text']
        self.sc.parallelize(row).saveToCassandra(self.keyspace, self.table, columns=columns)
        rdd = self.rdd().select(*columns)
        self.assertEqual(rdd.count(), 1)
        return rdd.first()


    def test_row(self):
        row = Row(key='selective-save-test-row', int=2, text='a', boolean=False)
        read = self._save_and_get(row)

        for k in ['key', 'text']:
            self.assertEqual(getattr(row, k), getattr(read, k))
        for k in ['boolean', 'int']:
            self.assertIsNone(getattr(read, k, None))


    def test_dict(self):
        row = dict(key='selective-save-test-row', int=2, text='a', boolean=False)
        read = self._save_and_get(row)

        for k in ['key', 'text']:
            self.assertEqual(row[k], read[k])
        for k in ['boolean', 'int']:
            self.assertIsNone(getattr(read, k, None))



class LimitAndTakeTest(SimpleTypesTestBase):
    size = 1000

    def setUp(self):
        super(LimitAndTakeTest, self).setUp()
        data = self.sc.parallelize(range(0, self.size)).map(lambda i: {'key':i, 'int':i})
        data.saveToCassandra(self.keyspace, self.table)

    def test_limit(self):
        data = self.rdd()

        for i in (5, 10, 100, 1000, 1500):
            l = min(i, self.size)
            self.assertEqual(len(data.take(i)), l)
            self.assertEqual(len(data.limit(i).collect()), l)
            self.assertEqual(len(data.limit(i * 2).take(i)), l)


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



class ConfTest(SimpleTypesTestBase):
    # TODO this is still a very basic test, more cases and (better) validation required
    def setUp(self):
        super(SimpleTypesTestBase, self).setUp()
        for i in range(100):
            self.session.execute(
                "INSERT INTO %s (key, text, int) values ('%s', '%s', %s)"
                % (self.table, i, i, i)
            )

    def test_read_conf(self):
        self.rdd(split_count=100).collect()
        self.rdd(split_size=32).collect()
        self.rdd(fetch_size=100).collect()
        self.rdd(consistency_level='LOCAL_QUORUM').collect()
        self.rdd(consistency_level=ConsistencyLevel.LOCAL_QUORUM).collect()
        self.rdd(metrics_enabled=True).collect()
        self.rdd(read_conf=ReadConf(split_count=10, consistency_level='ALL')).collect()
        self.rdd(read_conf=ReadConf(consistency_level='ALL', metrics_enabled=True)).collect()

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
    interval = .1

    size = 10
    count = 3

    rows = [
        [
            {'key': str(j * size + i), 'text': str(j * size + i)}
            for i in range(size)
        ]
        for j in range(count)
    ]

    @classmethod
    def setUpClass(cls):
        super(StreamingTest, cls).setUpClass()
        cls.ssc = StreamingContext(cls.sc, cls.interval)

    def setUp(self):
        super(StreamingTest, self).setUp()
        self.rdds = list(map(self.sc.parallelize, self.rows))
        self.stream = self.ssc.queueStream(self.rdds)

    def test(self):
        self.stream.saveToCassandra(self.keyspace, self.table)

        self.ssc.start()
        self.ssc.awaitTermination((self.count + 1) * self.interval)
        self.ssc.stop(stopSparkContext=False, stopGraceFully=True)

        tbl = self.rdd(row_format=RowFormat.TUPLE).select('key', 'text')
        read = tbl.by_primary_key().collect()
        self.assertEqual(len(read), self.size * self.count)
        for (k, v) in read:
            self.assertEqual(k, v)


class JoinRDDTest(SimpleTypesTestBase):

    def setUp(self):
        super(JoinRDDTest, self).setUp()

    def test_simple_pk(self):
        table = 'join_rdd_test_simple_pk'

        self.session.execute('''
            CREATE TABLE IF NOT EXISTS ''' + table + ''' (
                key text primary key, value text
            )
        ''')
        self.session.execute('TRUNCATE %s' % table)

        rows = {
           str(c) : str(i) for i, c in
           enumerate(string.ascii_lowercase)
        }

        for k, v in rows.items():
            self.session.execute(
                'INSERT INTO ' + table + ' (key, value) values (%s, %s)', (k, v)
            )

        rdd = self.sc.parallelize(rows.items())
        self.assertEqual(dict(rdd.collect()), rows)

        tbl = rdd.joinWithCassandraTable(self.keyspace, table)
        joined = tbl.on('key').select('key', 'value').cache()
        self.assertEqual(dict(joined.keys().collect()), dict(joined.values().collect()))
        for (k, v) in joined.collect():
            self.assertEqual(k, v)


    def test_composite_pk(self):
        table = 'join_rdd_test_composite_pk'

        self.session.execute('''
            CREATE TABLE IF NOT EXISTS ''' + table + ''' (
                pk text, cc text, value text,
                primary key (pk, cc)
            )
        ''')
        self.session.execute('TRUNCATE %s' % table)

        rows = [
           # (pk, cc, pk + '-' + cc)
           (unicode(pk), unicode(cc), unicode(pk + '-' + cc))
           for pk in string.ascii_lowercase[:3]
           for cc in (str(i) for i in range(3))
        ]

        for row in rows:
            self.session.execute(
                'INSERT INTO ' + table + ' (pk, cc, value) values (%s, %s, %s)', row
            )

        rdd = self.sc.parallelize(rows)

        joined = rdd.joinWithCassandraTable(self.keyspace, table).on('pk', 'cc')
        self.assertEqual(sorted(zip(rows, rows)), sorted(joined.map(tuple).collect()))

        joined = rdd.joinWithCassandraTable(self.keyspace, table).on('pk')
        self.assertEqual(len(rows) * sqrt(len(rows)), joined.count())


        # TODO test
        # .where()
        # .limit()




class JoinDStreamTest(StreamingTest):
    def setUp(self):
        super(JoinDStreamTest, self).setUp()
        self.joined_rows = self.sc.accumulator([], accum_param=AddingAccumulatorParam([]))

    def checkRDD(self, time, rdd):
        self.joined_rows += rdd.collect()

    def test(self):
        rows = list(chain(*self.rows))
        rows_by_key = {row['key'] : row for row in rows}

        self.sc \
            .parallelize(rows) \
            .saveToCassandra(self.keyspace, self.table)

        self.stream \
            .joinWithCassandraTable(self.keyspace, self.table, ['text'], ['key']) \
            .foreachRDD(self.checkRDD)

        self.ssc.start()
        self.ssc.awaitTermination((self.count + 1) * self.interval)
        self.ssc.stop(stopSparkContext=False, stopGraceFully=True)

        joined_rows = self.joined_rows.value
        self.assertEqual(len(joined_rows), len(rows))
        for row in joined_rows:
            self.assertEqual(len(row), 2)
            left, right = row

            self.assertEqual(type(left), type(right))
            self.assertEqual(rows_by_key[left['key']], left)
            self.assertEqual(left['text'], right['text'])
            self.assertEqual(len(right), 1)



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

    def test_89(self):
        self.session.execute('''
            CREATE TABLE IF NOT EXISTS test_89 (
                id text PRIMARY KEY,
                val text
            )
        ''')
        self.session.execute('''TRUNCATE test_89''')

        self.sc.parallelize([dict(id='a', val='b')]).saveToCassandra(self.keyspace, 'test_89')
        joined = (self.sc
            .parallelize([dict(id='a', uuid=UUID('27776620-e46e-11e5-a837-0800200c9a66'))])
            .joinWithCassandraTable(self.keyspace, 'test_89')
            .collect()
        )

        self.assertEqual(len(joined), 1)
        self.assertEqual(len(joined[0]), 2)
        left, right = joined[0]
        self.assertEqual(left['id'], 'a')
        self.assertEqual(left['uuid'], UUID('27776620-e46e-11e5-a837-0800200c9a66'))
        self.assertEqual(right['id'], 'a')
        self.assertEqual(right['val'], 'b')

    def test_93(self):
        self.session.execute('''
            CREATE TABLE IF NOT EXISTS test_93 (
                name text,
                data_final blob,
                data_inter blob,
                family_label text,
                rand double,
                source text,
                score float,
                PRIMARY KEY (name)
            )
        ''')

        self.sc.parallelize([
            Row(name=str(i), data_final=bytearray(str(i)), data_inter=bytearray(str(i)),
                family_label=str(i), rand=i / 10, source=str(i), score=i * 10)
            for i in range(4)
        ]).saveToCassandra(self.keyspace, 'test_93')

        joined = (self.sc
            .parallelize([
                Row(name='1', score=0.4),
                Row(name='2', score=0.5),
            ])
            .joinWithCassandraTable(self.keyspace, 'test_93')
            .on('name').collect()
        )

        self.assertEqual(len(joined), 2)




if __name__ == '__main__':
    try:
        # connect to cassandra and create a keyspace for testing
        CassandraTestCase.session = Cluster().connect()
        CassandraTestCase.session.execute('''
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        ''' % (CassandraTestCase.keyspace,))
        CassandraTestCase.session.set_keyspace(CassandraTestCase.keyspace)

        # create a cassandra spark context
        CassandraTestCase.sc = CassandraSparkContext(conf=SparkConf().setAppName("PySpark Cassandra Test"))

        # perform the unit tests
        unittest.main()
        # suite = unittest.TestLoader().loadTestsFromTestCase(RegressionTest)
        # unittest.TextTestRunner().run(suite)
    finally:
        # stop the spark context and cassandra session
        CassandraTestCase.sc.stop()
        CassandraTestCase.session.shutdown()

