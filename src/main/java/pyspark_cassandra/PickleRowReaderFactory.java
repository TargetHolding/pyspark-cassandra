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
import java.util.HashMap;
import java.util.Map;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import scala.Option;
import scala.collection.Seq;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public class PickleRowReaderFactory implements RowReaderFactory<byte[]>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public RowReader<byte[]> rowReader(TableDef tbl, RowReaderOptions opts) {
		return new PickleRowReader();
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

		@Override
		public Option<Object> requiredColumns() {
			return Option.apply(null);
		}

		@Override
		public byte[] read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
			if (this.pickler == null) {
				this.pickler = new Pickler();
			}

			Map<String, Object> map = new HashMap<String, Object>();

			for (int i = 0; i < columnNames.length; i++) {
				map.put(columnNames[i],
						row.getColumnDefinitions().getType(i).deserialize(row.getBytesUnsafe(i), protocolVersion));
			}

			try {
				return pickler.dumps(map);
			} catch (PickleException | IOException e) {
				// TODO clean up
				throw new RuntimeException(e);
			}
		}

		@Override
		public Option<Object> consumedColumns() {
			return Option.apply(null);
		}

		@Override
		public Option<Seq<String>> columnNames() {
			return Option.apply(null);
		}

	}
}