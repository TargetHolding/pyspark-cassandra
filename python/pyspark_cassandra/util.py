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



from collections import Set, Iterable, Mapping
from datetime import datetime
from time import mktime

from pyspark_cassandra.types import UDT


def as_java_array(gateway, java_type, iterable):
    """Creates a Java array from a Python iterable, using the given p4yj gateway"""

    if iterable is None:
        return None

    java_type = gateway.jvm.__getattr__(java_type)
    lst = list(iterable)
    arr = gateway.new_array(java_type, len(lst))

    for i, e in enumerate(lst):
        jobj = as_java_object(gateway, e)
        arr[i] = jobj

    return arr


def as_java_object(gateway, obj):
    """Converts a limited set of types to their corresponding types in java. Supported are 'primitives' (which aren't
    converted), datetime.datetime and the set-, dict- and iterable-like types.
    """

    if obj is None:
        return None

    t = type(obj)

    if issubclass(t, (bool, int, float, str)):
        return obj

    elif issubclass(t, UDT):
        field_names = as_java_array(gateway, "String", obj.keys())
        field_values = as_java_array(gateway, "Object", obj.values())
        udt = gateway.jvm.UDTValueConverter(field_names, field_values)
        return udt.toConnectorType()

    elif issubclass(t, datetime):
        timestamp = int(mktime(obj.timetuple()) * 1000)
        return gateway.jvm.java.util.Date(timestamp)

    elif issubclass(t, (dict, Mapping)):
        hash_map = gateway.jvm.java.util.HashMap()
        for (k, v) in obj.items(): hash_map[k] = v
        return hash_map

    elif issubclass(t, (set, Set)):
        hash_set = gateway.jvm.java.util.HashSet()
        for e in obj: hash_set.add(e)
        return hash_set

    elif issubclass(t, (list, Iterable)):
        array_list = gateway.jvm.java.util.ArrayList()
        for e in obj: array_list.append(e)
        return array_list

    else:
        return obj


def load_class(ctx, name):
    return ctx._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass(name)

_helper = None

def helper(ctx):
    global _helper

    if not _helper:
        _helper = load_class(ctx, "pyspark_cassandra.PythonHelper").newInstance()

    return _helper
