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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class KVDictsRowReaderFactory implements RowReaderFactory<Object[]>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public KVDictsRowReader rowReader(TableDef tbl, RowReaderOptions opts) {
		return new KVDictsRowReader(tbl, opts);
	}

	@Override
	public RowReaderOptions rowReader$default$2() {
		return RowReaderOptions.Default();
	}

	@Override
	public Class<Object[]> targetClass() {
		return Object[].class;
	}

	private final class KVDictsRowReader extends BaseRowReader<Object[]> {
		private static final long serialVersionUID = 1L;

		public KVDictsRowReader(TableDef tbl, RowReaderOptions opts) {
			super(tbl, opts);
		}

		@Override
		public Object[] read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			List<String> keyColumns = intersect(columnNames, tbl.primaryKey());
			List<String> valueColumns = intersect(columnNames, tbl.regularColumns());

			Map<String, Object> key = new HashMap<String, Object>(keyColumns.size());
			Map<String, Object> value = new HashMap<String, Object>(valueColumns.size());

			for (String keyColumn : keyColumns) {
				key.put(keyColumn, readColumn(keyColumn, row, protocolVersion));
			}

			for (String valueColumn : valueColumns) {
				value.put(valueColumn, readColumn(valueColumn, row, protocolVersion));
			}

			return new Object[] { key, value };
		}
	}
}