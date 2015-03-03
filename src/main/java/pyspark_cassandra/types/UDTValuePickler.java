package pyspark_cassandra.types;

import com.datastax.spark.connector.UDTValue;

import scala.collection.IndexedSeq;

public class UDTValuePickler extends StructPickler {
	public String getCreator() {
		return "pyspark_cassandra.types\n_create_udt\n";
	}

	@Override
	public IndexedSeq<String> getFieldNames(Object o) {
		return ((UDTValue)o).fieldNames();
	}

	@Override
	public IndexedSeq<Object> getFieldValues(Object o) {
		return ((UDTValue)o).fieldValues();
	}
}
