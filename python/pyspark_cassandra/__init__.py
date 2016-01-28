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

"""
    This module provides python support for Apache Spark's Resillient Distributed Datasets from
    Apache Cassandra CQL rows using the Spark Cassandra Connector from
    https://github.com/datastax/spark-cassandra-connector.
"""

import inspect

import pyspark.context
import pyspark.rdd

import pyspark_cassandra.context
import pyspark_cassandra.rdd

from .conf import WriteConf
from .context import CassandraSparkContext, monkey_patch_sc
from .rdd import RowFormat
from .types import Row, UDT


__all__ = [
    "CassandraSparkContext",
    "ReadConf",
    "Row",
    "RowFormat",
    "streaming",
    "UDT",
    "WriteConf"
]


# Monkey patch the default python RDD so that it can be stored to Cassandra as CQL rows
from .rdd import saveToCassandra, joinWithCassandraTable
pyspark.rdd.RDD.saveToCassandra = saveToCassandra
pyspark.rdd.RDD.joinWithCassandraTable = joinWithCassandraTable

# Monkey patch the sc variable in the caller if any
frame = inspect.currentframe().f_back
# Go back at most 10 frames
for _ in range(10):
    if not frame:
        break
    elif "sc" in frame.f_globals:
        monkey_patch_sc(frame.f_globals["sc"])
        break
    else:
        frame = frame.f_back
