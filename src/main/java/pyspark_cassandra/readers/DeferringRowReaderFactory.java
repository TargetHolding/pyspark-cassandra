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

import pyspark_cassandra.types.RawRow;
import scala.Option;
import scala.collection.Seq;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class DeferringRowReaderFactory implements RowReaderFactory<RawRow>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public RowReader<RawRow> rowReader(TableDef tableDef, RowReaderOptions options) {
		return new DeferringRowReader(tableDef);
	}

	@Override
	public RowReaderOptions rowReader$default$2() {
		return RowReaderOptions.Default();
	}

	@Override
	public Class<RawRow> targetClass() {
		return RawRow.class;
	}

	private final class DeferringRowReader implements RowReader<RawRow> {
		private static final long serialVersionUID = 1L;
		
		private TableDef tableDef;

		public DeferringRowReader(TableDef tableDef) {
			this.tableDef = tableDef;
		}

		@Override
		public RawRow read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			return new RawRow(row, columnNames, tableDef, protocolVersion);
		}

		@Override
		public Option<Seq<String>> columnNames() {
			return Option.apply(null);
		}

		@Override
		public Option<Object> consumedColumns() {
			return Option.apply(null);
		}

		@Override
		public Option<Object> requiredColumns() {
			return Option.apply(null);
		}
	}
}