package pyspark_cassandra.types;

import java.util.ArrayList;
import java.util.List;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;

import org.apache.spark.sql.cassandra.CassandraSQLRow;

import scala.collection.immutable.IndexedSeq;
import akka.japi.Util;

public class CassandraSQLRowUnpickler implements IObjectConstructor {
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object construct(Object[] args) throws PickleException {
		List<String> columnNames = (ArrayList<String>) args[0];
		List<Object> fieldValues = args[1] instanceof List ? (List) args[1] : Types.toList((Object[]) args[1]);

		IndexedSeq<String> columnNamesSeq = Util.immutableIndexedSeq(columnNames);
		IndexedSeq<Object> fieldValuesSeq = Util.immutableIndexedSeq(fieldValues);
		return new CassandraSQLRow(columnNamesSeq, fieldValuesSeq);
	}
}
