# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import copy
from datetime import timedelta, datetime, date

from pyspark.rdd import RDD
from pyspark.serializers import PickleSerializer
from pyspark_cassandra.types import as_java_array


class RowFormat(object):
	"""An enumeration of CQL row formats used in CassandraRDD"""
	
	DICT = 0
	TUPLE = 1
	KV_DICTS = 2
	KV_TUPLES = 3
	ROW = 4
	
	values = (DICT, TUPLE, KV_DICTS, KV_TUPLES, ROW)


class CassandraRDD(RDD):
	"""A Resillient Distributed Dataset of Cassandra CQL rows. As any RDD objects of this class are immutable; i.e.
	operations on this RDD generate a new RDD."""
	
	def __init__(
			self, keyspace, table, ctx, row_format=None, split_size=None, fetch_size=None,
			consistency_level=None, metrics_enabled=None
		):
		
		self.keyspace = keyspace
		self.table = table
		
		if not row_format:
			row_format = RowFormat.ROW
		elif row_format < 0 or row_format >= len(RowFormat.values):
			raise ValueError("invalid row_format %s" % row_format)

		jvm = ctx._jvm
		ReadConf = jvm.ReadConf
		
		split_size = split_size or ReadConf.DefaultSplitSize()
		fetch_size = fetch_size or ReadConf.DefaultFetchSize()
		consistency_level = jvm.ConsistencyLevel.values()[consistency_level] \
			if consistency_level else ReadConf.DefaultConsistencyLevel()
		
		# TODO metrics_enabled = ReadConf.DefaultReadTaskMetricsEnabled() \
		#	if metrics_enabled is None else metrics_enabled
		metrics_enabled = False
		
		read_conf = ReadConf(
			split_size,
			fetch_size,
			consistency_level,
			metrics_enabled,
		)
		
		row_format = ctx._jvm.RowFormat.values()[row_format]
		reader_factory = ctx._jvm.PickleRowReaderFactory(row_format)
		jrdd = (
			ctx._cjcs \
				.cassandraTable(keyspace, table, reader_factory)
				.withReadConf(read_conf)
		)
		
		super(CassandraRDD, self).__init__(jrdd, ctx, PickleSerializer())


	def select(self, *columns):
		"""Creates a CassandraRDD with the select clause applied.""" 

		columns = as_java_array(self.ctx._gateway, "String", (str(c) for c in columns))
		new = copy(self)
		new._jrdd = new._jrdd.select(columns)
		return new


	def where(self, clause, *args):
		"""Creates a CassandraRDD with a CQL where clause applied.
		@param clause: The where clause, either complete or with ? markers
		@param *args: The parameters for the ? markers in the where clause.
		"""

		args = as_java_array(self.ctx._gateway, "Object", args)
		new = copy(self)
		new._jrdd = new._jrdd.where(clause, args)
		return new
	
	def __copy__(self):
		c = CassandraRDD.__new__(CassandraRDD)
		c.__dict__.update(self.__dict__)
		return c
	
def saveToCassandra(
		rdd, keyspace=None, table=None, columns=None,
		batch_size=None, batch_buffer_size=None, batch_grouping_key=None,
		consistency_level=None, parallelism_level=None, throughput_mibps=None,
		ttl=None, timestamp=None, metrics_enabled=None, row_format=None
	):
	"""
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
	@param batch_size(int):
		The size in bytes to batch up in an unlogged batch of CQL inserts.
		If None given the default size of 16*1024 is used or spark.cassandra.output.batch.size.bytes if set.
	@param batch_buffer_size(int):
		The maximum number of batches which are 'pending'.
		If None given the default of 1000 is used.
	@param batch_grouping_key(string):
		The way batches are formed:
		* all: any row can be added to any batch
    	* replicaset: rows are batched for replica sets 
    	* partition: rows are batched by their partition key
    	* None: defaults to "partition"
	@param consistency_level(cassandra.ConsistencyLevel):
		The consistency level used in writing to Cassandra.
		If None defaults to LOCAL_ONE or spark.cassandra.output.consistency.level if set.
	@param parallelism_level(int):
		The maximum number of batches written in parallel.
		If None defaults to 8 or spark.cassandra.output.concurrent.writes if set.
	@param throughput_mibps(int):
	@param ttl(int or timedelta):
		The time to live as milliseconds or timedelta to use for the values.
		If None given no TTL is used.
	@param timestamp(int, date or datetime):
		The timestamp in milliseconds, date or datetime to use for the values.
		If None given the Cassandra nodes determine the timestamp.
	@param row_format(RowFormat):
		Make explicit how to map the RDD elements into Cassandra rows.
		If None given the mapping is auto-detected as far as possible.
	"""
	
	keyspace = keyspace or rdd.keyspace
	table = table or rdd.table
	
	if not keyspace:
		raise ValueError("keyspace not set")
	
	if not table:
		raise ValueError("table not set")

	# convert timedelta ttl to milliseconds
	if ttl and isinstance(ttl, timedelta):
		ttl = int(ttl.total_seconds() * 1000)

	# convert date or datetime objects to a timestamp in milliseconds since the UNIX epoch
	if timestamp and isinstance(timestamp, datetime) or isinstance(timestamp, date):
		epoch = timestamp.__class__(1970, 1, 1)
		timestamp = int((timestamp - epoch).total_seconds() * 1000)

	# get the WriteConf class from the py4j JVM view
	jvm = rdd.ctx._jvm
	WriteConf = jvm.WriteConf

	# determine the various values for WriteConf
	# unfortunately the default values in WriteConf can't be used through py4j 
	batch_size = jvm.BytesInBatch(batch_size or WriteConf.DefaultBatchSizeInBytes())
	batch_buffer_size = batch_buffer_size or WriteConf.DefaultBatchGroupingBufferSize()
	batch_grouping_key = jvm.__getattr__("BatchGroupingKey$").__getattr__("MODULE$").apply(batch_grouping_key) \
		if batch_grouping_key else WriteConf.DefaultBatchGroupingKey()

	consistency_level = jvm.ConsistencyLevel.values()[consistency_level] \
		if consistency_level else WriteConf.DefaultConsistencyLevel()

	parallelism_level = parallelism_level or WriteConf.DefaultParallelismLevel()
	throughput_mibps = throughput_mibps or WriteConf.DefaultThroughputMiBPS()

	ttl = jvm.TTLOption.constant(ttl) if ttl else jvm.TTLOption.defaultValue()
	timestamp = jvm.TimestampOption.constant(timestamp) if timestamp else jvm.TimestampOption.defaultValue()

	# TODO metrics_enabled = WriteConf.DefaultWriteTaskMetricsEnabled() if metrics_enabled is None else metrics_enabled
	metrics_enabled = False

	# create the WriteConf object
	write_conf = WriteConf(
		batch_size,
		batch_buffer_size,
		batch_grouping_key,
		consistency_level,
		parallelism_level,
		throughput_mibps,
		ttl,
		timestamp,
		metrics_enabled
	)
	
	# determine the row format
	row_format = jvm.RowFormat.values()[row_format] if row_format else None

	# perform the actual saveToCassandra using the write_conf built before
	rdd._reserialize(PickleSerializer()) \
		.ctx._jvm.CassandraJavaUtil.javaFunctions(rdd._jrdd) \
		.writerBuilder(keyspace, table, rdd.ctx._jvm.PickleRowWriterFactory(row_format)) \
		.withWriteConf(write_conf) \
		.saveToCassandra()

