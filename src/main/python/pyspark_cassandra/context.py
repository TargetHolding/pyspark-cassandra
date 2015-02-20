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

from functools import partial

from py4j.java_gateway import java_import
import pyspark.context
from pyspark_cassandra.rdd import CassandraRDD, RowFormat


def convert(sc):
	sc.__class__ = CassandraSparkContext
	sc.__dict__["cassandraTable"] = partial(CassandraSparkContext.cassandraTable, sc)
	_init_cassandra_spark_context(sc)


def _init_cassandra_spark_context(sc):
	jvm = sc._jvm
	java_import(jvm, "pyspark_cassandra.*")
	java_import(jvm, "com.datastax.driver.core.*")
	java_import(jvm, "com.datastax.spark.connector.*")
	java_import(jvm, "com.datastax.spark.connector.japi.CassandraJavaUtil")
	java_import(jvm, "com.datastax.spark.connector.writer.*")
	
	sc._cjcs = jvm.CassandraJavaUtil.javaFunctions(sc._jsc)



class CassandraSparkContext(pyspark.context.SparkContext):
	"""Wraps a SparkContext which allows reading CQL rows from Cassandra"""

	def _do_init(self, *args, **kwargs):
		super(CassandraSparkContext, self)._do_init(*args, **kwargs)
		_init_cassandra_spark_context(self)

	def cassandraTable(self, keyspace, table, row_format = RowFormat.DICT):
		"""Returns a CassandraRDD for the given keyspace and table"""
		return CassandraRDD(keyspace, table, self, row_format)
