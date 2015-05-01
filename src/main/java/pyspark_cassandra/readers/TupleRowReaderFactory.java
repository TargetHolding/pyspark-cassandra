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

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class TupleRowReaderFactory implements RowReaderFactory<Object[]>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public TupleRowReader rowReader(TableDef tbl, RowReaderOptions opts) {
		return new TupleRowReader(tbl, opts);
	}

	@Override
	public RowReaderOptions rowReader$default$2() {
		return RowReaderOptions.Default();
	}

	@Override
	public Class<Object[]> targetClass() {
		return Object[].class;
	}

	private final class TupleRowReader extends BaseRowReader<Object[]> {
		private static final long serialVersionUID = 1L;

		public TupleRowReader(TableDef tbl, RowReaderOptions opts) {
			super(tbl, opts);
		}

		@Override
		public Object[] read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			Object[] tuple = new Object[columnNames.length];

			for (int i = 0; i < columnNames.length; i++) {
				tuple[i] = readColumn(i, row, protocolVersion);
			}

			return tuple;
		}
	}
}