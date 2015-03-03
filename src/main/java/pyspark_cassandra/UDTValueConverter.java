package pyspark_cassandra;

import pyspark_cassandra.types.Types;
import scala.collection.IndexedSeq;

import com.datastax.spark.connector.UDTValue;

public class UDTValueConverter {
	private String[] fieldNames;
	private Object[] fieldValues;

	public UDTValueConverter(String[] fieldNames, Object[] fieldValues) {
		super();
		this.fieldNames = fieldNames;
		this.fieldValues = fieldValues;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public Object[] getFieldValues() {
		return fieldValues;
	}

	public UDTValue toConnectorType() {
		IndexedSeq<String> fieldNamesSeq = Types.toArraySeq(this.fieldNames);
		IndexedSeq<Object> fieldValuesSeq = Types.toArraySeq(this.fieldValues);
		return new UDTValue(fieldNamesSeq, fieldValuesSeq);
	}
}
