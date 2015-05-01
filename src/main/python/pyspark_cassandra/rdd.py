# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#	 http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import copy
from datetime import timedelta, datetime, date

from pyspark.rdd import RDD
from pyspark.serializers import BatchedSerializer, PickleSerializer
from pyspark_cassandra.types import as_java_array
from pyspark_cassandra.conf import WriteConf, ReadConf


class RowFormat(object):
	"""An enumeration of CQL row formats used in CassandraRDD"""
	
	DICT = 0
	TUPLE = 1
	KV_DICTS = 2
	KV_TUPLES = 3
	ROW = 4
	
	values = (DICT, TUPLE, KV_DICTS, KV_TUPLES, ROW)


# class OneByOneSerializer(Serializer):
# 	def dump_stream(self, iterator, stream):
# 		"""
# 		Serialize an iterator of objects to the output stream.
# 		"""
# 		raise NotImplementedError
# 
# 	def load_stream(self, stream):
# 		"""
# 		Return an iterator of deserialized objects from the input stream.
# 		"""
# 		raise NotImplementedError


class CassandraRDD(RDD):
	'''
		A Resilient Distributed Dataset of Cassandra CQL rows. As any RDD, objects of this class are immutable; i.e.
		operations on this RDD generate a new RDD.
	'''
	
	def __init__(self, keyspace, table, ctx, row_format=None, read_conf=None):
		self.keyspace = keyspace
		self.table = table
		
		if not row_format:
			reader_factory = ctx._jvm.CassandraRowReaderFactory()
		elif row_format < 0 or row_format >= len(RowFormat.values):
			raise ValueError("invalid row_format %s" % row_format)
		elif row_format == RowFormat.ROW:
			reader_factory = ctx._jvm.CassandraRowReaderFactory()
		elif row_format == RowFormat.TUPLE:
			reader_factory = ctx._jvm.TupleRowReaderFactory()
		elif row_format == RowFormat.DICT:
			reader_factory = ctx._jvm.DictRowReaderFactory()
		elif row_format == RowFormat.KV_TUPLES:
			reader_factory = ctx._jvm.KVTuplesRowReaderFactory()
		elif row_format == RowFormat.KV_DICTS:
			reader_factory = ctx._jvm.KVDictsRowReaderFactory()
					
		# build the CassandraRDD
		self._cjrdd = (
			ctx._cjcs
				.cassandraTable(keyspace, table, reader_factory)
				.withReadConf((read_conf or ReadConf(ctx)).to_java_conf())
		)
		
		# The CassandraRDD is batch pickled by mapping each partition (more efficient then one by one)
		# This requires memorizing the CassandraRDD because select and where only operate on that RDD
		# and not on a JavaRDD which is returned by mapPartitions
		jrdd = self._cjrdd.mapPartitions(ctx._jvm.BatchPickle(), False)
		super(CassandraRDD, self).__init__(jrdd, ctx)


	def select(self, *columns):
		"""Creates a CassandraRDD with the select clause applied.""" 

		columns = as_java_array(self.ctx._gateway, "String", (str(c) for c in columns))
		new = copy(self)
		new._cjrdd = new._cjrdd.select(columns)
		new.jrdd = new._cjrdd.mapPartitions(self.ctx._jvm.BatchPickle(), False)
		return new


	def where(self, clause, *args):
		"""Creates a CassandraRDD with a CQL where clause applied.
		@param clause: The where clause, either complete or with ? markers
		@param *args: The parameters for the ? markers in the where clause.
		"""

		args = as_java_array(self.ctx._gateway, "Object", args)
		new = copy(self)
		new._cjrdd = new._cjrdd.where(clause, args)
		new.jrdd = new._cjrdd.mapPartitions(self.ctx._jvm.BatchPickle(), False)
		return new
	
	def __copy__(self):
		c = CassandraRDD.__new__(CassandraRDD)
		c.__dict__.update(self.__dict__)
		return c
	
	
def saveToCassandra(rdd, keyspace=None, table=None, columns=None, write_conf=None, row_format=None):
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
		
		@param write_conf(WriteConf):
			A WriteConf object to use when saving to Cassandra
		
		@param row_format(RowFormat):
			Make explicit how to map the RDD elements into Cassandra rows.
			If None given the mapping is auto-detected as far as possible.
	'''
	
	keyspace = keyspace or rdd.keyspace
	table = table or rdd.table
	
	if not keyspace:
		raise ValueError("keyspace not set")
	
	if not table:
		raise ValueError("table not set")

	# determine the row format
	row_format = rdd.ctx._jvm.RowFormat.values()[row_format] if row_format else None

	# 	builder = rdd._reserialize(BatchedSerializer()) \
	# 		.ctx._jvm.CassandraJavaUtil.javaFunctions(rdd._jrdd) \
	# 		.writerBuilder(keyspace, table, rdd.ctx._jvm.PickleRowWriterFactory(row_format))

	# unpickle the batches in the JVM
	unpickled = rdd._jrdd.flatMap(rdd.ctx._jvm.BatchUnpickle())

	# create a builder for saving to cassandra
	builder = rdd.ctx._jvm \
		.CassandraJavaUtil.javaFunctions(unpickled) \
		.writerBuilder(keyspace, table, rdd.ctx._jvm.ObjectRowWriterFactory(row_format))
	
	# set the write config if given
	# TODO this can be optional, but we set the metrics_enabled to a different default than the Spark Cassandra
	# Connector, so we construct a default if none given.
	builder = builder.withWriteConf((write_conf or WriteConf(rdd.ctx)).to_java_conf())
	
	# perform the actual saveToCassandra	
	builder.saveToCassandra()

