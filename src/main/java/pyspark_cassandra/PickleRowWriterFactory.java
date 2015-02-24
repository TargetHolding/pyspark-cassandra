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

package pyspark_cassandra;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.cassandra.CassandraSQLRow;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.custom.Unpickler;
import scala.collection.IndexedSeq;
import scala.collection.Seq;
import akka.japi.Util;

import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;

public class PickleRowWriterFactory implements RowWriterFactory<byte[]>, Serializable {
	private static final long serialVersionUID = 1L;

	static {
		Unpickler.registerConstructor("uuid", "UUID", new UUIDUnpickler());
		Unpickler.registerConstructor("pyspark.sql", "_create_row", new CassandraSQLRowUnpickler());
		Unpickler.registerConstructor("pyspark_cassandra.row", "_create_row", new CassandraSQLRowUnpickler());
	}

	private RowFormat format;

	public PickleRowWriterFactory(RowFormat format) {
		this.format = format;
	}

	@Override
	public RowWriter<byte[]> rowWriter(TableDef table, Seq<String> columnNames) {
		return new PickleRowWriter(columnNames, format);
	}

	private final class PickleRowWriter implements RowWriter<byte[]> {
		private static final long serialVersionUID = 1L;

		private transient Unpickler unpickler;
		private Seq<String> columnNames;
		private RowFormat format;

		public PickleRowWriter(Seq<String> columnNames, RowFormat format) {
			this.columnNames = columnNames;
			this.format = format;
		}

		@Override
		public Seq<String> columnNames() {
			return columnNames;
		}

		@Override
		public void readColumnValues(byte[] pickledRow, Object[] buffer) {
			if (this.unpickler == null) {
				this.unpickler = new Unpickler();
			}

			try {
				Object row = unpickle(pickledRow);

				RowFormat format = this.format;
				if (format == null) {
					format = this.detectFormat(row);
				}

				switch (format) {
				case DICT:
					readAsDict((Map<?, ?>) row, buffer);
					break;
				case TUPLE:
					readAsTuple((Object[]) row, buffer);
					break;
				case KV_DICTS:
					readAsKeyValueDicts((Object[]) row, buffer);
					break;
				case KV_TUPLES:
					readAsKeyValueTuples((Object[]) row, buffer);
					break;
				case ROW:
					readAsRow((CassandraSQLRow) row, buffer);
					break;
				}
			} catch (PickleException | IOException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
		}

		private void readAsDict(Map<?, ?> row, Object[] buffer) {
			for (int idx = 0; idx < this.columnNames.length(); idx++) {
				buffer[idx] = row.get(this.columnNames.apply(idx));
			}
		}

		private void readAsTuple(Object[] row, Object[] buffer) {
			System.arraycopy(row, 0, buffer, 0, Math.min(row.length, buffer.length));
		}

		private void readAsKeyValueDicts(Object[] row, Object[] buffer) {
			Map<?, ?> key = (Map<?, ?>) row[0];
			Map<?, ?> value = (Map<?, ?>) row[1];

			for (int idx = 0; idx < this.columnNames.length(); idx++) {
				String columnName = this.columnNames.apply(idx);
				Object val = key.get(columnName);

				if (val == null) {
					val = value.get(columnName);
				}

				buffer[idx] = val;
			}
		}

		private void readAsKeyValueTuples(Object[] row, Object[] buffer) {
			Object[] key = (Object[]) row[0];
			Object[] value = (Object[]) row[1];

			int keyLength = Math.min(key.length, buffer.length);
			System.arraycopy(key, 0, buffer, 0, keyLength);
			System.arraycopy(value, 0, buffer, keyLength, Math.min(value.length, buffer.length - keyLength));
		}

		private void readAsRow(CassandraSQLRow row, Object[] buffer) {
			IndexedSeq<String> names = row.fieldNames();
			IndexedSeq<Object> values = row.fieldValues();

			for (int i = 0; i < names.size(); i++) {
				int idx = this.columnNames.indexOf(names.apply(i));
				if (idx >= 0) {
					buffer[idx] = values.apply(i);
				}
			}
		}

		private Object unpickle(byte[] pickledRow) throws IOException {
			Object unpickled = unpickler.loads(pickledRow);

			if (unpickled instanceof List) {
				List<?> list = (List<?>) unpickled;

				// TODO proper exceptions
				if (list.size() > 1) {
					throw new RuntimeException("Can't write a list of rows in one go ... must be a map!");
				}

				return list.get(0);
			} else {
				return unpickled;
			}
		}

		private RowFormat detectFormat(Object row) {
			// The detection works because primary keys can't be maps, sets or lists. If the detection still fails, a
			// user must set the row_format explicitly

			// CassandraSQLRow map to ROW of course
			if (row instanceof CassandraSQLRow) {
				return RowFormat.ROW;
			}

			// If the row is a map, the only possible format is DICT
			else if (row instanceof Map) {
				return RowFormat.DICT;
			}

			// otherwise it must be a tuple
			else if (row instanceof Object[]) {
				Object[] tuple = (Object[]) row;

				// If the row is a tuple of length two, try to figure out if it's a (key,value) tuple
				if (tuple.length == 2) {
					// if both tuple elements are maps, the format must be KV_DICTS, since primary keys can't be maps
					if (tuple[0] instanceof Map && tuple[1] instanceof Map) {
						return RowFormat.KV_DICTS;
					}

					// if both tuple elements are tuples themselves, the format must be KV_TUPLES
					else if (tuple[0] instanceof Object[] && tuple[1] instanceof Object[]) {
						return RowFormat.KV_TUPLES;
					}
				}

				// if falling through, attempt to store it as a regular tuple
				return RowFormat.TUPLE;
			}

			// or if falling through, we are unable to detect the row
			throw new RuntimeException("Unable to detect or unsupported row format for " + row
					+ ". Set it explicitly with saveToCassandra(..., row_format=...).");
		}
	}
}