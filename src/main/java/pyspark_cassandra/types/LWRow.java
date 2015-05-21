/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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
