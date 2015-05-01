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
import java.util.Map;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class DictRowReaderFactory implements RowReaderFactory<Map<String, Object>>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public DictRowReader rowReader(TableDef tbl, RowReaderOptions opts) {
		return new DictRowReader(tbl, opts);
	}

	@Override
	public RowReaderOptions rowReader$default$2() {
		return RowReaderOptions.Default();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Class<Map<String, Object>> targetClass() {
		return (Class<Map<String,Object>>)(Class<?>)Map.class;
	}

	private final class DictRowReader extends BaseRowReader<Map<String, Object>> {
		private static final long serialVersionUID = 1L;

		public DictRowReader(TableDef tbl, RowReaderOptions opts) {
			super(tbl, opts);
		}

		@Override
		public Map<String, Object> read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			Map<String, Object> dict = new HashMap<String, Object>();

			for (int i = 0; i < columnNames.length; i++) {
				dict.put(columnNames[i], readColumn(i, row, protocolVersion));
			}

			return dict;
		}
	}
}