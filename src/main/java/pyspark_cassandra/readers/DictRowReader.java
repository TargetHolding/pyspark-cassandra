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

package pyspark_cassandra.readers;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;

public class DictRowReader extends RowReader<Map<String, Object>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Map<String, Object> parse(Row row, String[] columnNames, TableDef tableDef, ProtocolVersion protocolVersion) {
		Map<String, Object> dict = new HashMap<String, Object>();

		for (int i = 0; i < columnNames.length; i++) {
			dict.put(columnNames[i], readColumn(i, row, protocolVersion));
		}

		return dict;
	}
}
