import pickle
import unittest

from pyspark_cassandra.types import Row, UDT


class _TestStruct:
	def test_empty_struct(self):
		try:
			self.cls()
			self.fail("Shouldn't be able to create an empty %s" % self.cls.__name__)
		except ValueError:
			pass
	
	def test_equality(self):
		self.assertNotEqual(self.cls(a="1"), self.cls(a="2"))
		self.assertNotEqual(self.cls(a="1"), self.cls(a="1", b="3"))
		
	def test_pickling(self):
		original = self.cls(a="1", b="2")
		pickled = pickle.dumps(original)
		unpickled = pickle.loads(pickled)
		self.assertEqual(original, unpickled)


class TestRow(_TestStruct, unittest.TestCase):
	cls = Row

class TestUDT(_TestStruct, unittest.TestCase):
	cls = UDT

if __name__ == '__main__':
	unittest.main()
