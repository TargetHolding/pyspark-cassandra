package pyspark_cassandra.types;

import org.apache.spark.sql.cassandra.CassandraSQLRow;

import scala.collection.IndexedSeq;

public class CassandraSQLRowPickler extends StructPickler {
	public String getCreator() {
		return "pyspark_cassandra.types\n_create_row\n";
	}

	@Override
	public IndexedSeq<String> getFieldNames(Object o) {
		return ((CassandraSQLRow) o).fieldNames();
	}

	@Override
	public IndexedSeq<Object> getFieldValues(Object o) {
		return ((CassandraSQLRow) o).fieldValues();
	}
}
