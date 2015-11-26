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

from types import as_java_object, as_java_array

from pyspark.streaming.dstream import DStream
from pyspark_cassandra.conf import WriteConf
from pyspark_cassandra.util import helper


def saveToCassandra(dstream, keyspace, table, columns=None, row_format=None, keyed=None, write_conf=None, **write_conf_kwargs):
    ctx = dstream._ssc._sc
    gw = ctx._gateway

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(gw, write_conf.settings())
    # convert the columns to a string array
    columns = as_java_array(gw, "String", columns) if columns else None

    helper(ctx).saveToCassandra(dstream._jdstream, keyspace, table, columns, row_format, keyed, write_conf)

# Monkey patch the default python DStream so that data in it can be stored to Cassandra as CQL rows
DStream.saveToCassandra = saveToCassandra
