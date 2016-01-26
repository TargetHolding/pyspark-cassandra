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

from datetime import datetime, tzinfo, timedelta
from itertools import chain
from operator import itemgetter
import struct


try:
    # import accessed as globals, see _create_spanning_dataframe(...)
    import numpy as np  # @UnusedImport
    import pandas as pd  # @UnusedImport
except ImportError:
    pass



def _create_row(fields, values):
    return _create_struct(Row, fields, values)

def _create_udt(fields, values):
    return _create_struct(UDT, fields, values)

def _create_struct(cls, fields, values):
    d = {k: v for k, v in zip(fields, values)}
    return cls(**d)


class Struct(tuple):
    """Adaptation from the pyspark.sql.Row which better supports adding fields"""

    def __new__(cls, **kwargs):
        if not kwargs:
            raise ValueError("Cannot construct empty %s" % cls)

        struct = tuple.__new__(cls)
        struct.__FIELDS__ = kwargs
        return struct


    def asDict(self):
        return self.__dict__()

    def __dict__(self):
        return self.__FIELDS__

    def __iter__(self):
        return iter(self.__FIELDS__.values())

    @property
    def _fields(self):
        return self.keys()

    def keys(self):
        return self.__FIELDS__.keys()

    def values(self):
        return self.__FIELDS__.values()


    def __len__(self):
        return len(self.__FIELDS__)

    def __hash__(self):
        h = 1
        for v in chain(self.keys(), self.values()):
            h = 31 * h + hash(v)
        return h

    def __eq__(self, other):
        try:
            return self.__FIELDS__.__eq__(other.__FIELDS__)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other


    def __add__(self, other):
        d = dict(self.__FIELDS__)
        d.update(other.__FIELDS__)
        return self.__class__(**d)

    def __sub__(self, other):
        d = { k:v for k, v in self.__FIELDS__.items() if k not in other }
        return self.__class__(**d)

    def __and__(self, other):
        d = { k:v for k, v in self.__FIELDS__.items() if k in other }
        return self.__class__(**d)


    def __contains__(self, name):
        return name in self.__FIELDS__


    def __setitem__(self, name, value):
        self.__setattr__(name, value)

    def __delitem__(self, name):
        self.__delattr__(name)

    def __getitem__(self, name):
        return self.__getattr__(name)


    def __getattr__(self, name):
        try:
            return self.__FIELDS__[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if name == "__FIELDS__":
            tuple.__setattr__(self, name, value)
        else:
            self.__FIELDS__[name] = value

    def __delattr__(self, name):
        try:
            del self.__FIELDS__[name]
        except KeyError:
            raise AttributeError(name)


    def __getstate__(self):
        return self.__dict__()

    def __reduce__(self):
        keys = list(self.__FIELDS__.keys())
        values = [self.__FIELDS__[k] for k in keys]
        return (self._creator(), (keys, values,))


    def __repr__(self):
        fields = sorted(self.__FIELDS__.items(), key=itemgetter(0))
        values = ", ".join("%s=%r" % (k, v) for k, v in fields if k != '__FIELDS__')
        return "%s(%s)" % (self.__class__.__name__, values)



class Row(Struct):
    def _creator(self):
        return _create_row

class UDT(Struct):
    def _creator(self):
        return _create_udt



def _create_spanning_dataframe(cnames, ctypes, cvalues):
    '''
        Constructs a 'dataframe' from column names, numpy column types and
        the column values.

        @param cnames: An iterable of name strings
        @param ctypes: An iterable of numpy dtypes as strings (e.g. '>f4')
        @param cvalues: An iterable of
        
        Note that cnames, ctypes and cvalues are expected to have equal length.
    '''

    if len(cnames) != len(ctypes) or len(ctypes) != len(cvalues):
        raise ValueError('The lengths of cnames, ctypes and cvalues must equal')

    # convert the column values to numpy arrays if numpy is available
    # otherwise use lists
    global np
    convert = _to_nparrays if np else _to_list
    arrays = {n : convert(t, v) for n, t, v in zip(cnames, ctypes, cvalues)}

    # if pandas is available, provide the arrays / lists as DataFrame
    # otherwise use pyspark_cassandra.Row
    global pd
    if pd:
        return pd.DataFrame(arrays)
    else:
        return Row(**arrays)


def _to_nparrays(ctype, cvalue):
    if isinstance(cvalue, (bytes, bytearray)):
        # The array is byte swapped and set to little-endian. java encodes
        # ints, longs, floats, etc. in big-endian.
        # This costs some cycles (around 1 ms per 1*10^6 elements) but when
        # using it it saves some when using the array (around 25 to 50 % which
        # for summing amounts to half a ms)
        # (the perf numbers above are on an Intel i5-4200M)
        # Also it solves an issue with pickling datetime64 arrays see
        # https://github.com/numpy/numpy/issues/5883
        return np.frombuffer(cvalue, ctype).byteswap(True).newbyteorder('<')
    else:
        return np.fromiter(cvalue, None)


def _to_list(ctype, cvalue):
    if isinstance(cvalue, (bytes, bytearray)):
        return _decode_primitives(ctype, cvalue)
    elif hasattr(cvalue, '__len__'):
        return cvalue
    else:
        return list(cvalue)

# from https://docs.python.org/3/library/datetime.html
ZERO = timedelta(0)

class UTC(tzinfo):
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

    def __repr__(self):
        return self.__class__.__name__

utc = UTC()


_numpy_to_struct_formats = {
    '>b1': '?',
    'i4': '>i',
    '>i8': '>q',
    '>f4': '>f',
    '>f8': '>d',
    '>M8[ms]': '>q',
}

def _decode_primitives(ctype, cvalue):
    fmt = _numpy_to_struct_formats.get(ctype)

    # if unsupported, return as the list if bytes it was
    if not fmt:
        return cvalue

    primitives = _unpack(fmt, cvalue)

    if ctype == '>M8[ms]':
        return [datetime.utcfromtimestamp(l).replace(tzinfo=UTC) for l in primitives]
    else:
        return primitives


def _unpack(fmt, cvalue):
    stride = struct.calcsize(fmt)
    if len(cvalue) % stride != 0:
        raise ValueError('number of bytes must be a multiple of %s for format %s' % (stride, fmt))

    return [struct.unpack(cvalue[o:o + stride]) for o in range(len(cvalue) / stride, stride)]

