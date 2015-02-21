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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import scala.Option;
import scala.collection.Seq;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.ColumnDef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class PickleRowReaderFactory implements RowReaderFactory<byte[]>, Serializable {
	private static final long serialVersionUID = 1L;

	private RowFormat format;

	public PickleRowReaderFactory(RowFormat format) {
		this.format = format;
	}

	@Override
	public RowReader<byte[]> rowReader(TableDef tbl, RowReaderOptions opts) {
		return new PickleRowReader(tbl, this.format);
	}

	@Override
	public RowReaderOptions rowReader$default$2() {
		return new RowReaderOptions(0);
	}

	@Override
	public Class<byte[]> targetClass() {
		return byte[].class;
	}

	private final class PickleRowReader implements RowReader<byte[]> {
		private static final long serialVersionUID = 1L;

		private transient Pickler pickler;

		private TableDef tbl;
		private RowFormat format;

		public PickleRowReader(TableDef tbl, RowFormat format) {
			this.tbl = tbl;
			this.format = format;
		}

		@Override
		public Option<Object> requiredColumns() {
			return Option.apply(null);
		}

		@Override
		public Option<Object> consumedColumns() {
			return Option.apply(null);
		}

		@Override
		public Option<Seq<String>> columnNames() {
			return Option.apply(null);
		}

		@Override
		public byte[] read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			if (this.pickler == null) {
				this.pickler = new Pickler();
			}

			Object ret = null;

			switch (this.format) {
			case DICT:
				ret = readAsDict(row, columnNames, protocolVersion);
				break;
			case TUPLE:
				ret = readAsTuple(row, columnNames, protocolVersion);
				break;
			case KV_DICTS:
				ret = readAsKeyValueDicts(row, columnNames, protocolVersion);
				break;
			case KV_TUPLES:
				ret = readAsKeyValueTuples(row, columnNames, protocolVersion);
				break;
			}

			try {
				return pickler.dumps(ret);
			} catch (PickleException | IOException e) {
				// TODO clean up
				throw new RuntimeException("Unable to pickle " + ret, e);
			}
		}

		private Map<String, Object> readAsDict(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			Map<String, Object> dict = new HashMap<String, Object>();

			for (int i = 0; i < columnNames.length; i++) {
				dict.put(columnNames[i], readColumn(i, row, protocolVersion));
			}

			return dict;
		}

		private Object[] readAsTuple(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			Object[] tuple = new Object[columnNames.length];

			for (int i = 0; i < columnNames.length; i++) {
				tuple[i] = readColumn(i, row, protocolVersion);
			}

			return tuple;
		}

		private Object[] readAsKeyValueDicts(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
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

		private Object[] readAsKeyValueTuples(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
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

		private Object readColumn(int idx, Row row, ProtocolVersion protocolVersion) {
			ByteBuffer bytes = row.getBytesUnsafe(idx);
			return bytes == null ? null : row.getColumnDefinitions().getType(idx).deserialize(bytes, protocolVersion);
		}

		private Object readColumn(String name, Row row, ProtocolVersion protocolVersion) {
			ByteBuffer bytes = row.getBytesUnsafe(name);
			return bytes == null ? null : row.getColumnDefinitions().getType(name).deserialize(bytes, protocolVersion);
		}

		private List<String> intersect(String[] columnNames, Seq<ColumnDef> def) {
			List<String> intersection = toList(columnNames);
			intersection.retainAll(columnNames(def));
			return intersection;
		}

		private List<String> columnNames(Seq<ColumnDef> def) {
			List<String> defs = new ArrayList<String>(def.size());

			for (int i = 0; i < def.size(); i++) {
				defs.add(def.apply(i).columnName());
			}
			return defs;
		}

		private <T> List<T> toList(T[] arr) {
			List<T> list = new ArrayList<T>(arr.length);

			for (T v : arr) {
				list.add(v);
			}

			return list;
		}
	}
}