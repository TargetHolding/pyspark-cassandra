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

import java.nio.ByteBuffer;

import pyspark_cassandra.types.LWRow;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;

public class LWRowReader extends RowReader<LWRow> {
	private static final long serialVersionUID = 1L;

	@Override
	public LWRow parse(Row row, String[] columnNames, TableDef tableDef, ProtocolVersion protocolVersion) {
		ColumnDefinitions defs = row.getColumnDefinitions();
		Object[] values = new Object[columnNames.length];

		for (int i = 0; i < values.length; i++) {
			if (!row.isNull(columnNames[i])) {
				ByteBuffer bytes = row.getBytesUnsafe(columnNames[i]);
				DataType type = defs.getType(columnNames[i]);
				values[i] = type.deserialize(bytes, protocolVersion);
			}
		}

		return new LWRow(columnNames, values);
	}
}
