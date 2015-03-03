package pyspark_cassandra.types;

import java.util.ArrayList;
import java.util.List;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import scala.collection.immutable.IndexedSeq;
import akka.japi.Util;

public abstract class StructUnpickler implements IObjectConstructor {
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object construct(Object[] args) throws PickleException {
		List<String> fieldNames = (ArrayList<String>) args[0];
		List<Object> fieldValues = args[1] instanceof List ? (List) args[1] : Types.toJavaList((Object[]) args[1]);

		IndexedSeq<String> fieldNamesSeq = Util.immutableIndexedSeq(fieldNames);
		IndexedSeq<Object> fieldValuesSeq = Util.immutableIndexedSeq(fieldValues);
		return this.construct(fieldNamesSeq, fieldValuesSeq);
	}

	public abstract Object construct(IndexedSeq<String> fieldNames, IndexedSeq<Object> fieldValues);
}
