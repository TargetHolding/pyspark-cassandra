package pyspark_cassandra.types;

import scala.collection.immutable.IndexedSeq;

import com.datastax.spark.connector.UDTValue;

public class UDTValueUnpickler extends StructUnpickler {
	@Override
	public Object construct(IndexedSeq<String> fieldNames, IndexedSeq<Object> fieldValues) {
		return new UDTValue(fieldNames, fieldValues);
	}
}
