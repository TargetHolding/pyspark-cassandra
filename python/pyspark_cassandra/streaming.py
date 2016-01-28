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

from pyspark_cassandra.util import as_java_object, as_java_array

from pyspark.streaming.dstream import DStream
from pyspark_cassandra.conf import WriteConf
from pyspark_cassandra.util import helper
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer


def saveToCassandra(dstream, keyspace, table, columns=None, row_format=None, keyed=None,
                    write_conf=None, **write_conf_kwargs):
    ctx = dstream._ssc._sc
    gw = ctx._gateway

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(gw, write_conf.settings())
    # convert the columns to a string array
    columns = as_java_array(gw, "String", columns) if columns else None

    return helper(ctx).saveToCassandra(dstream._jdstream, keyspace, table, columns, row_format,
                                       keyed, write_conf)


def joinWithCassandraTable(dstream, keyspace, table, selected_columns=None, join_columns=None):
    """Joins a DStream (a stream of RDDs) with a Cassandra table

    Arguments:
        @param dstream(DStream)
        The DStream to join. Equals to self when invoking joinWithCassandraTable on a monkey
        patched RDD.
        @param keyspace(string):
            The keyspace to join on.
        @param table(string):
            The CQL table to join on.
        @param selected_columns(string):
            The columns to select from the Cassandra table.
        @param join_columns(string):
            The columns used to join on from the Cassandra table.
    """

    ssc = dstream._ssc
    ctx = ssc._sc
    gw = ctx._gateway

    selected_columns = as_java_array(gw, "String", selected_columns) if selected_columns else None
    join_columns = as_java_array(gw, "String", join_columns) if join_columns else None

    h = helper(ctx)
    dstream = h.joinWithCassandraTable(dstream._jdstream, keyspace, table, selected_columns,
                                       join_columns)
    dstream = h.pickleRows(dstream)
    dstream = h.javaDStream(dstream)

    return DStream(dstream, ssc, AutoBatchedSerializer(PickleSerializer()))


# Monkey patch the default python DStream so that data in it can be stored to and joined with
# Cassandra tables
DStream.saveToCassandra = saveToCassandra
DStream.joinWithCassandraTable = joinWithCassandraTable
