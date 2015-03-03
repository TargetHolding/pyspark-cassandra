from operator import itemgetter
from datetime import datetime
from time import mktime
from collections import Set, Iterable, Mapping



def _create_row(fields, values):
	return _create_struct(Row, fields, values)

def _create_udt(fields, values):
	return _create_struct(UDT, fields, values)

def _create_struct(cls, fields, values):
	d = dict((k, v) for k, v in zip(fields, values))
	return cls(**d)


class Struct(tuple):
	"""Adaptation from the pyspark.sql.Row which better supports adding fields"""

	def __new__(cls, *args, **kwargs):
		if args and kwargs:
			raise ValueError("Cannot use both args and kwargs to create a Row")
		elif args:
			return tuple.__new__(cls, args)
		elif kwargs:
			struct = tuple.__new__(cls)
			struct.__FIELDS__ = kwargs
			return struct
		else:
			raise ValueError("Cannot construct empty %s" % cls)
	
	
	def asDict(self):
		return self.__dict__()

	def __dict__(self):
		return self.__FIELDS__ if hasattr(self, "__FIELDS__") else {}
		
		
	def __len__(self):
		return len(self.__FIELDS__) if hasattr(self, "__FIELDS__") else tuple.__len__(self)
	
	def __eq__(self, other):
		if hasattr(self, "__FIELDS__"):
			return self.__FIELDS__.__eq__(other.__FIELDS__)
		else:
			return tuple.__eq__(self, other)
		
	def __ne__(self, other):
		return not self == other
	
	
	def __getattr__(self, name):
		if name.startswith("__"):
			raise AttributeError(name)
		try:
			return self.__FIELDS__[name]
		except KeyError:
			raise AttributeError(name)
	
	def __setattr__(self, name, value):
		if hasattr(self, "__FIELDS__"):
			self.__FIELDS__[name] = value
		tuple.__setattr__(self, name, value)
		
	
	def __getstate__(self):
		return self.__dict__()
		
	def __reduce__(self):
		if hasattr(self, "__FIELDS__"):
			keys = self.__FIELDS__.keys()
			values = [self.__FIELDS__[k] for k in keys]
			return (self._creator(), (keys, values,))
		else:
			return tuple.__reduce__(self)


	def __repr__(self, *args, **kwargs):
		if hasattr(self, "__FIELDS__"):
			fields = sorted(self.__FIELDS__.items(), key=itemgetter(0))
			values = ", ".join("%s=%r" % (k, v) for k, v in fields if k != '__FIELDS__')
		else:
			values = ", ".join(str(v) for v in self)
		
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
	
	if isinstance(obj, (bool, int, float, str)):
		return obj
	elif isinstance(obj, UDT):
		field_names = as_java_array(gateway, "String", obj.__dict__().keys())
		field_values = as_java_array(gateway, "Object", obj.__dict__().values())
		udt = gateway.jvm.UDTValueConverter(field_names, field_values)
		return udt.toConnectorType()
	elif isinstance(obj, datetime):
		timestamp = int(mktime(obj.timetuple()) * 1000)
		return gateway.jvm.java.util.Date(timestamp)
	elif isinstance(obj, (set, Set)) or(hasattr(obj, 'intersection') and  hasattr(obj, '__iter__')):
		hash_set = gateway.jvm.java.util.HashSet()
		for e in obj: hash_set.add(e)
		return hash_set
	elif isinstance(obj, (dict, Mapping)) or hasattr(obj, 'iteritems'):
		hash_map = gateway.jvm.java.util.HashMap()
		for (k, v) in obj.items(): hash_map[k] = v
		return hash_map
	elif isinstance(obj, (list, Iterable)) or hasattr(obj, '__iter__'):
		array_list = gateway.jvm.java.util.ArrayList()
		for e in obj: array_list.append(e)
		return array_list
	else:
		return obj


