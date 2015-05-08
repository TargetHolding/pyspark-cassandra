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

import pyspark_cassandra.types.LWRow;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class LWRowReaderFactory implements RowReaderFactory<LWRow>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public RowReader<LWRow> rowReader(TableDef tbl, RowReaderOptions opts) {
		return new DriverRowReader(tbl, opts);
	}

	@Override
	public RowReaderOptions rowReader$default$2() {
		return RowReaderOptions.Default();
	}

	@Override
	public Class<LWRow> targetClass() {
		return LWRow.class;
	}

	private final class DriverRowReader extends BaseRowReader<LWRow> {
		private static final long serialVersionUID = 1L;

		public DriverRowReader(TableDef tbl, RowReaderOptions opts) {
			super(tbl, opts);
		}

		@Override
		public LWRow read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			ColumnDefinitions defs = row.getColumnDefinitions();
			Object[] values = new Object[columnNames.length];

			for (int i = 0; i < values.length; i++) {
				values[i] = defs.getType(columnNames[i]).deserialize(row.getBytesUnsafe(columnNames[i]), protocolVersion);
			}

			return new LWRow(columnNames, values);
		}
	}
}