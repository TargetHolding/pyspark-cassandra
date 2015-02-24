package pyspark_cassandra;

import java.util.ArrayList;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;

import org.apache.spark.sql.cassandra.CassandraSQLRow;

import scala.collection.immutable.IndexedSeq;
import akka.japi.Util;

public class CassandraSQLRowUnpickler implements IObjectConstructor {
	@Override
	public Object construct(Object[] args) throws PickleException {
		@SuppressWarnings("unchecked")
		IndexedSeq<String> columnNames = Util.immutableIndexedSeq((ArrayList<String>) args[0]);
		IndexedSeq<Object> fieldValues = Util.immutableIndexedSeq(Types.toList((Object[])args[1]));
		return new CassandraSQLRow(columnNames, fieldValues);
	}
}
