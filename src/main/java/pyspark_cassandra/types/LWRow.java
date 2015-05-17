package pyspark_cassandra.types;

import java.util.Arrays;
import java.util.List;

public class LWRow {
	private String[] fields;
	private Object[] values;

	public LWRow(String[] fields, Object[] values) {
		this.fields = fields;
		this.values = values;
	}

	public LWRow(List<String> fields, List<Object> values) {
		// TODO consider another approach to remove this conceptually unnecessary toArray calls
		this(fields.toArray(new String[fields.size()]), values.toArray());
	}

	public String[] getFieldsNames() {
		return fields;
	}

	public Object[] getFieldValues() {
		return values;
	}

	@Override
	public String toString() {
		return "LWRow [fields=" + Arrays.toString(fields) + ", values=" + Arrays.toString(values) + "]";
	}
}
