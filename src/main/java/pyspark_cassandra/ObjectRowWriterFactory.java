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

import java.io.Serializable;
import java.util.Map;

import pyspark_cassandra.types.LWRow;
import scala.collection.IndexedSeq;
import scala.collection.Seq;

import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;

public class ObjectRowWriterFactory implements RowWriterFactory<Object>, Serializable {
	private static final long serialVersionUID = 1L;

	private RowFormat format;

	public ObjectRowWriterFactory(RowFormat format) {
		this.format = format;
	}

	public RowWriter<Object> rowWriter(TableDef table, Seq<String> columnNames,
			scala.collection.immutable.Map<String, String> aliasToColumnName) {
		return this.rowWriter(table, columnNames);
	}

	public RowWriter<Object> rowWriter(TableDef table, Seq<String> columnNames) {
		return new ObjectRowWriter(columnNames, format);
	}

	private final class ObjectRowWriter implements RowWriter<Object> {
		private static final long serialVersionUID = 1L;

		private Seq<String> columnNames;
		private RowFormat format;

		public ObjectRowWriter(Seq<String> columnNames, RowFormat format) {
			this.columnNames = columnNames;
			this.format = format;
		}

		@Override
		public Seq<String> columnNames() {
			return columnNames;
		}

		@Override
		public void readColumnValues(Object row, Object[] buffer) {
			RowFormat format = this.format;
			if (format == null) {
				this.format = this.detectFormat(row);
			}

			switch (this.format) {
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
				readAsRow((LWRow) row, buffer);
				break;
			case KV_ROWS:
				readAsKeyValueRows((LWRow[]) row, buffer);
				break;
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

		// private void readAsRow(CassandraRow row, Object[] buffer) {
		// IndexedSeq<String> names = row.fieldNames();
		// IndexedSeq<Object> values = row.fieldValues();
		//
		// for (int i = 0; i < names.size(); i++) {
		// int idx = this.columnNames.indexOf(names.apply(i));
		// if (idx >= 0) {
		// buffer[idx] = values.apply(i);
		// }
		// }
		// }

		private void readAsRow(LWRow row, Object[] buffer) {
			readAsRow(row, buffer, 0);
		}

		private void readAsKeyValueRows(LWRow[] row, Object[] buffer) {
			readAsRow(row[0], buffer, 0);
			readAsRow(row[1], buffer, row[0].getFieldsNames().length);
		}

		private void readAsRow(LWRow row, Object[] buffer, int offset) {
			String[] names = row.getFieldsNames();
			Object[] values = row.getFieldValues();

			for (int i = 0; i < names.length; i++) {
				int idx = this.columnNames.indexOf(names[i]);
				if (idx >= 0) {
					buffer[idx + offset] = values[i];
				}
			}
		}

		private RowFormat detectFormat(Object row) {
			// The detection works because primary keys can't be maps, sets or lists. If the detection still fails, a
			// user must set the row_format explicitly

			// CassandraRow map to ROW of course
			if (row instanceof CassandraRow) {
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