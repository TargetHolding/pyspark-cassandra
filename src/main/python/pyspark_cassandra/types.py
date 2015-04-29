from collections import Set, Iterable, Mapping
from datetime import datetime
from operator import itemgetter
from time import mktime


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
	
	def keys(self):
		return self.__FIELDS__.keys()

	def values(self):
		return self.__FIELDS__.values()
		
		
	def __len__(self):
		return len(self.__FIELDS__)
	
	def __eq__(self, other):
		return self.__FIELDS__.__eq__(other.__FIELDS__)
		
	def __ne__(self, other):
		return not self == other
	
	
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
		keys = self.__FIELDS__.keys()
		values = [self.__FIELDS__[k] for k in keys]
		return (self._creator(), (keys, values,))


	def __repr__(self, *args, **kwargs):
		fields = sorted(self.__FIELDS__.items(), key=itemgetter(0))
		values = ", ".join("%s=%r" % (k, v) for k, v in fields if k != '__FIELDS__')
		return "%s(%s)" % (self.__class__.__name__, values)



class Row(Struct):
	def _creator(self):
		return _create_row
	
	
class UDT(Struct):
	def _creator(self):
		return _create_udt





def as_java_array(gateway, java_type, iterable):
	"""Creates a Java array from a Python iterable, using the given p4yj gateway"""

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
	
	elif issubclass(t, (list, Iterable)):
		array_list = gateway.jvm.java.util.ArrayList()
		for e in obj: array_list.append(e)
		return array_list
	
	elif issubclass(t, (dict, Mapping)):
		hash_map = gateway.jvm.java.util.HashMap()
		for (k, v) in obj.items(): hash_map[k] = v
		return hash_map
	
	elif issubclass(t, (set, Set)):
		hash_set = gateway.jvm.java.util.HashSet()
		for e in obj: hash_set.add(e)
		return hash_set
	
	else:
		return obj


