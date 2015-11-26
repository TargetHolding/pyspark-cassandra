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

from copy import copy
from itertools import groupby
from operator import itemgetter
import sys

from pyspark.rdd import RDD
from pyspark_cassandra.conf import WriteConf, ReadConf
from pyspark_cassandra.format import RowFormat, ColumnSelector
from pyspark_cassandra.types import as_java_array, as_java_object, Row
from pyspark_cassandra.util import helper


if sys.version_info > (3,):
    long = int # @ReservedAssignment


try:
    import pandas as pd # @UnusedImport, import used in SpanningRDD
except:
    pass


def saveToCassandra(rdd, keyspace=None, table=None, columns=None, row_format=None, keyed=None, write_conf=None,
                    **write_conf_kwargs):
    '''
        Saves an RDD to Cassandra. The RDD is expected to contain dicts with keys mapping to CQL columns.

        Arguments:
        @param rdd(RDD):
            The RDD to save. Equals to self when invoking saveToCassandra on a monkey patched RDD.
        @param keyspace(string):in
            The keyspace to save the RDD in. If not given and the rdd is a CassandraRDD the same keyspace is used.
        @param table(string):
            The CQL table to save the RDD in. If not given and the rdd is a CassandraRDD the same table is used.

        Keyword arguments:
        @param columns(iterable):
            The columns to save, i.e. which keys to take from the dicts in the RDD.
            If None given all columns are be stored.

        @param row_format(RowFormat):
            Make explicit how to map the RDD elements into Cassandra rows.
            If None given the mapping is auto-detected as far as possible.
        @param keyed(bool):
            Make explicit that the RDD consists of key, value tuples (and not arrays of length two).

        @param write_conf(WriteConf):
            A WriteConf object to use when saving to Cassandra
        @param **write_conf_kwargs:
            WriteConf parameters to use when saving to Cassandra
    '''

    keyspace = keyspace or getattr(rdd, 'keyspace', None)
    if not keyspace:
        raise ValueError("keyspace not set")

    table = table or getattr(rdd, 'table', None)
    if not table:
        raise ValueError("table not set")

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(rdd.ctx._gateway, write_conf.settings())
    # convert the columns to a string array
    columns = as_java_array(rdd.ctx._gateway, "String", columns) if columns else None

    helper(rdd.ctx) \
        .saveToCassandra(
            rdd._jrdd,
            keyspace,
            table,
            columns,
            row_format,
            keyed,
            write_conf,
        )


class _CassandraRDD(RDD):
    '''
        A Resilient Distributed Dataset of Cassandra CQL rows. As any RDD, objects of this class are immutable; i.e.
        operations on this RDD generate a new RDD.
    '''

    def __init__(self, ctx, keyspace, table, row_format=None, read_conf=None, **read_conf_kwargs):
        if not keyspace:
            raise ValueError("keyspace not set")

        if not table:
            raise ValueError("table not set")

        if row_format is None:
            row_format = RowFormat.ROW
        elif row_format < 0 or row_format >= len(RowFormat.values):
            raise ValueError("invalid row_format %s" % row_format)

        self.keyspace = keyspace
        self.table = table
        self.row_format = row_format
        self.read_conf = ReadConf.build(read_conf, **read_conf_kwargs)
        self._limit = None

        # this jrdd is for compatibility with pyspark.rdd.RDD
        # while allowing this constructor to be use for type checking etc
        # and setting _jrdd //after// invoking this constructor
        class DummyJRDD(object):
            def id(self):
                return -1
        jrdd = DummyJRDD()

        super(_CassandraRDD, self).__init__(jrdd, ctx)


    @property
    def _helper(self):
        return helper(self.ctx)


    def _pickle_jrdd(self):
        jrdd = self._helper.pickleRows(self._crdd, self.row_format)
        return self._helper.javaRDD(jrdd)


    def get_crdd(self):
        return self._crdd

    def set_crdd(self, crdd):
        self._crdd = crdd
        self._jrdd = self._pickle_jrdd()
        self._id = self._jrdd.id

    crdd = property(get_crdd, set_crdd)


    saveToCassandra = saveToCassandra


    def select(self, *columns):
        """Creates a CassandraRDD with the select clause applied."""
        columns = as_java_array(self.ctx._gateway, "String", (str(c) for c in columns))
        return self._specialize('select', columns)


    def where(self, clause, *args):
        """Creates a CassandraRDD with a CQL where clause applied.
        @param clause: The where clause, either complete or with ? markers
        @param *args: The parameters for the ? markers in the where clause.
        """
        return self._specialize('where', *[clause, args])


    def limit(self, limit):
        """Creates a CassandraRDD with the limit clause applied."""
        self._limit = limit
        return self._specialize('limit', long(limit))


    def take(self, num):
        """Takes at most 'num' records from the Cassandra table.

        Note that if limit() was invoked before take() a normal pyspark take()
        is performed. Otherwise, first limit is set and _then_ a take() is
        performed.
        """
        if self._limit:
            return super(_CassandraRDD, self).take(num)
        else:
            return self.limit(num).take(num)


    def cassandraCount(self):
        """Lets Cassandra perform a count, instead of loading data to Spark"""
        return self._crdd.cassandraCount()


    def _specialize(self, func_name, *args, **kwargs):
        func = getattr(self._helper, func_name)

        new = copy(self)
        new.crdd = func(new._crdd, *args, **kwargs)

        return new


    def spanBy(self, *columns):
        """"Groups rows by the given columns without shuffling.

        @param *columns: an iterable of columns by which to group.

        Note that:
        -    The rows are grouped by comparing the given columns in order and
            starting a new group whenever the value of the given columns changes.
            This works well with using the partition keys and one or more of the
            clustering keys. Use rdd.groupBy(...) for any other grouping.
        -    The grouping is applied on the partition level. I.e. any grouping
            will be a subset of its containing partition.
        """

        return SpanningRDD(self.ctx, self._crdd, self._jrdd, self._helper, columns)


    def __copy__(self):
        c = self.__class__.__new__(self.__class__)
        c.__dict__.update(self.__dict__)
        return c



class CassandraTableScanRDD(_CassandraRDD):
    def __init__(self, ctx, keyspace, table, row_format=None, read_conf=None, **read_conf_kwargs):
        super(CassandraTableScanRDD, self).__init__(ctx, keyspace, table, row_format, read_conf, **read_conf_kwargs)

        self._key_by = ColumnSelector.none()

        read_conf = as_java_object(ctx._gateway, self.read_conf.settings())

        self.crdd = self._helper \
            .cassandraTable(
                ctx._jsc,
                keyspace,
                table,
                read_conf,
            )


    def by_primary_key(self):
        return self.key_by(primary_key=True)

    def key_by(self, primary_key=True, partition_key=False, *columns):
        # TODO implement keying by arbitrary columns
        if columns:
            raise NotImplementedError('keying by arbitrary columns is not (yet) supported')
        if partition_key:
            raise NotImplementedError('keying by partition key is not (yet) supported')

        new = copy(self)
        new._key_by = ColumnSelector(partition_key, primary_key, *columns)
        new.crdd = self.crdd

        return new


    def _pickle_jrdd(self):
        # TODO implement keying by arbitrary columns
        jrdd = self._helper.pickleRows(self.crdd, self.row_format, self._key_by.primary_key)
        return self._helper.javaRDD(jrdd)



class SpanningRDD(RDD):
    '''
        An RDD which groups rows with the same key (as defined through named
        columns) within each partition.
    '''
    def __init__(self, ctx, crdd, jrdd, helper, columns):
        self._crdd = crdd
        self.columns = columns
        self._helper = helper

        rdd = RDD(jrdd, ctx).mapPartitions(self._spanning_iterator())
        super(SpanningRDD, self).__init__(rdd._jrdd, ctx)


    def _spanning_iterator(self):
        ''' implements basic spanning on the python side operating on Rows '''
        # TODO implement in Java and support not only Rows

        columns = set(str(c) for c in self.columns)

        def spanning_iterator(partition):
            def key_by(columns):
                for row in partition:
                    k = Row(**{c: row.__getattr__(c) for c in columns})
                    for c in columns:
                        del row[c]

                    yield (k, row)

            for g, l in groupby(key_by(columns), itemgetter(0)):
                yield g, list(_[1] for _ in l)

        return spanning_iterator


    def asDataFrames(self, *index_by):
        '''
            Reads the spanned rows as DataFrames if pandas is available, or as
            a dict of numpy arrays if only numpy is available or as a dict with
            primitives and objects otherwise.

            @param index_by If pandas is available, the dataframes will be
            indexed by the given columns.
        '''
        for c in index_by:
            if c in self.columns:
                raise ValueError('column %s cannot be used as index in the data'
                    'frames as it is a column by which the rows are spanned.')

        columns = as_java_array(self.ctx._gateway, "String", (str(c) for c in self.columns))
        jrdd = self._helper.spanBy(self._crdd, columns)
        rdd = RDD(jrdd, self.ctx)

        global pd
        if index_by and pd:
            return rdd.mapValues(lambda _: _.set_index(*[str(c) for c in index_by]))
        else:
            return rdd


def joinWithCassandraTable(left_rdd, keyspace, table):
    '''
        Join an RDD with a Cassandra table on the partition key. Use .on(...)
        to specifiy other columns to join on. .select(...), .where(...) and
        .limit(...) can be used as well.

        Arguments:
        @param left_rdd(RDD):
            The RDD to join. Equals to self when invoking joinWithCassandraTable on a monkey patched RDD.
        @param keyspace(string):
            The keyspace to join on
        @param table(string):
            The CQL table to join on.
    '''

    return CassandraJoinRDD(left_rdd, keyspace, table)


class CassandraJoinRDD(_CassandraRDD):
    '''
        TODO
    '''

    def __init__(self, left_rdd, keyspace, table):
        super(CassandraJoinRDD, self).__init__(left_rdd.ctx, keyspace, table)
        self.crdd = self._helper \
            .joinWithCassandraTable(
                left_rdd._jrdd,
                keyspace,
                table
            )


    def on(self, *columns):
        columns = as_java_array(self.ctx._gateway, "String", (str(c) for c in columns))
        return self._specialize('on', columns)
