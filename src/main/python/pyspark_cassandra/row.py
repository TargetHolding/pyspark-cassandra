from operator import itemgetter

def _create_row(fields, values):
	d = dict((k,v) for k,v in zip(fields, values))
	return Row(**d)

class Row(tuple):
	"""Adaptation from the pyspark.sql.Row which better supports adding fields"""

	def __new__(cls, *args, **kwargs):
		if args and kwargs:
			raise ValueError("Cannot use both args and kwargs to create a Row")
		elif args:
			row = tuple.__new__(cls, args)
			return row
		elif kwargs:
			row = tuple.__new__(cls)
			row.__FIELDS__ = kwargs
			return row
		else:
			raise ValueError("Cannot construct empty row")
	
	def asDict(self):
		return self.__dict__

	def __dict__(self):
		return self.__FIELDS__ if hasattr(self, "__FIELDS__") else {}
	
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
		
	def __reduce__(self):
		if hasattr(self, "__FIELDS__"):
			keys = self.__FIELDS__.keys()
			values = [self.__FIELDS__[k] for k in keys]
			return (_create_row, (keys, values,))
		else:
			return tuple.__reduce__(self)

	def __repr__(self, *args, **kwargs):
		if hasattr(self, "__FIELDS__"):
			fields = sorted(self.__FIELDS__.items(), key=itemgetter(0))
			return "Row(%s)" % ", ".join("%s=%r" % (k, v) for k, v in fields if k != '__FIELDS__')
		else:
			return "Row(%s)" % ", ".join(str(v) for v in self)

