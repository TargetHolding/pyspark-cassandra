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
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;

public class KVDictsRowReader extends KVRowReader<Map<String, Object>> {
	private static final long serialVersionUID = 1L;

	@Override
	protected Map<String, Object> parse(Row row, List<String> columnNames, TableDef tableDef, ProtocolVersion protocol) {
		Map<String, Object> key = new HashMap<String, Object>(columnNames.size());

		for (String keyColumn : columnNames) {
			key.put(keyColumn, readColumn(keyColumn, row, protocol));
		}

		return key;
	}
}
