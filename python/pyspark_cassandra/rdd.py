# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# 	 http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import copy
from itertools import groupby
from operator import itemgetter

from pyspark.rdd import RDD
from pyspark_cassandra.types import as_java_array, as_java_object, Row


try:
	import pandas as pd  # import used in SpanningRDD
except:
	pass


class RowFormat(object):
	"""An enumeration of CQL row formats used in CassandraRDD"""
	
	DICT = 0
	TUPLE = 1
	KV_DICTS = 2
	KV_TUPLES = 3
	ROW = 4
	KV_ROWS = 5
	
	values = (DICT, TUPLE, KV_DICTS, KV_TUPLES, ROW, KV_ROWS)


class CassandraRDD(RDD):
	'''
		A Resilient Distributed Dataset of Cassandra CQL rows. As any RDD, objects of this class are immutable; i.e.
		operations on this RDD generate a new RDD.
	'''
	
	def __init__(self, keyspace, table, ctx, row_format=None, read_conf=None):
		if not keyspace:
			raise ValueError("keyspace not set")
		
		if not table:
			raise ValueError("table not set")
		
		if not row_format:
			row_format = RowFormat.ROW
		elif row_format < 0 or row_format >= len(RowFormat.values):
			raise ValueError("invalid row_format %s" % row_format)
		
		self.keyspace = keyspace
		self.table = table
		self.read_conf = read_conf
		self.row_format = row_format
		
		jread_conf = as_java_object(ctx._gateway, read_conf.__dict__) if read_conf else None
		
		self._helper = ctx._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
			.loadClass("pyspark_cassandra.PythonHelper").newInstance()
		
		self._cjrdd = self._helper \
			.cassandraTable(
				keyspace,
				table,
				ctx._jsc,
				jread_conf,
			)
			
		jrdd = self._helper.parseRows(self._cjrdd, row_format)
		
		super(CassandraRDD, self).__init__(jrdd, ctx)


	def select(self, *columns):
		"""Creates a CassandraRDD with the select clause applied.""" 

		new = copy(self)
		new._cjrdd = new._cjrdd.select(as_java_array(self.ctx._gateway, "String", (str(c) for c in columns)))
		new._jrdd = self._helper.parseRows(new._cjrdd, self.row_format)
		return new


	def limit(self, rows_number):
		"""Creates a CassandraRDD with the limit clause applied."""

		new = copy(self)
		new._cjrdd = new._cjrdd.limit(long(rows_number))
		new._jrdd = self._helper.parseRows(new._cjrdd, self.row_format)
		return new


	def where(self, clause, *args):
		"""Creates a CassandraRDD with a CQL where clause applied.
		@param clause: The where clause, either complete or with ? markers
		@param *args: The parameters for the ? markers in the where clause.
		"""

		args = as_java_array(self.ctx._gateway, "Object", args)
		new = copy(self)
		new._cjrdd = new._cjrdd.where(clause, args)
		new._jrdd = self._helper.parseRows(new._cjrdd, self.row_format)
		return new
		
		
	def spanBy(self, *columns):
		""""Groups rows by the given columns without shuffling.

		@param *columns: an iterable of columns by which to group.
		
		Note that:
		-	The rows are grouped by comparing the given columns in order and
			starting a new group whenever the value of the given columns changes.
			This works well with using the partition keys and one or more of the
			clustering keys. Use rdd.groupBy(...) for any other grouping.  
		-	The grouping is applied on the partition level. I.e. any grouping
			will be a subset of its containing partition.
		"""
		
		return SpanningRDD(self.ctx, self._cjrdd, self._jrdd, self._helper, columns)
	
	
	def __copy__(self):
		c = CassandraRDD.__new__(CassandraRDD)
		c.__dict__.update(self.__dict__)
		return c


class SpanningRDD(RDD):
	'''
		An RDD which groups rows with the same key (as defined through named
		columns) within each partition.
	'''
	def __init__(self, ctx, cjrdd, jrdd, helper, columns):
		self._cjrdd = cjrdd
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
		jrdd = self._helper.spanBy(self._cjrdd, columns)
		rdd = RDD(jrdd, self.ctx)
		
		global pd
		if index_by and pd:
			return rdd.mapValues(lambda _: _.set_index(*[str(c) for c in index_by]))
		else:
			return rdd

	
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
	if not keyspace:
		raise ValueError("keyspace not set")
	
	table = table or rdd.table
	if not table:
		raise ValueError("table not set")
	
	# create write config as map and convert the columns to a string array
	write_conf = as_java_object(rdd.ctx._gateway, write_conf.__dict__) if write_conf else None
	columns = as_java_array(rdd.ctx._gateway, "String", columns) if columns else None

	# create a helper object
	helper = rdd.ctx._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
		.loadClass("pyspark_cassandra.PythonHelper").newInstance()
		
	# delegate to helper
	helper \
		.saveToCassandra(
			rdd._jrdd,
			keyspace,
			table,
			columns,
			write_conf,
			row_format
		)

