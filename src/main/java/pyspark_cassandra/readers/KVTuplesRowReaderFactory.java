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
import java.util.List;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class KVTuplesRowReaderFactory implements RowReaderFactory<Object[]>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public KVTuplesRowReader rowReader(TableDef tbl, RowReaderOptions opts) {
		return new KVTuplesRowReader(tbl, opts);
	}

	@Override
	public RowReaderOptions rowReader$default$2() {
		return RowReaderOptions.Default();
	}

	@Override
	public Class<Object[]> targetClass() {
		return Object[].class;
	}

	private final class KVTuplesRowReader extends BaseRowReader<Object[]> {
		private static final long serialVersionUID = 1L;
		
		public KVTuplesRowReader(TableDef tbl, RowReaderOptions opts) {
			super(tbl, opts);
		}

		@Override
		public Object[] read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			List<String> keyColumns = intersect(columnNames, tbl.primaryKey());
			List<String> valueColumns = intersect(columnNames, tbl.regularColumns());

			Object[] key = new Object[keyColumns.size()];
			Object[] value = new Object[valueColumns.size()];

			for (int i = 0; i < keyColumns.size(); i++) {
				key[i] = readColumn(keyColumns.get(i), row, protocolVersion);
			}

			for (int i = 0; i < valueColumns.size(); i++) {
				value[i] = readColumn(valueColumns.get(i), row, protocolVersion);
			}

			return new Object[] { key, value };
		}
	}
}