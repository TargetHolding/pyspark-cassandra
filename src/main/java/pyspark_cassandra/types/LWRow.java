package pyspark_cassandra.types;

public class LWRow {
	private String[] fields;
	private Object[] values;

	public LWRow(String[] fields, Object[] values) {
		this.fields = fields;
		this.values = values;
	}

	public String[] getFieldsNames() {
		return fields;
	}

	public Object[] getFieldValues() {
		return values;
	}
}
