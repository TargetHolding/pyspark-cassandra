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

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Unpickler;
import scala.collection.Seq;

import com.datastax.spark.connector.BatchSize;
import com.datastax.spark.connector.BytesInBatch;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.BatchLevel;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;

public class PickleRowWriterFactory implements RowWriterFactory<byte[]>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public RowWriter<byte[]> rowWriter(TableDef table, Seq<String> columnNames) {
		return new PickleRowWriter(columnNames);
	}

	private final class PickleRowWriter implements RowWriter<byte[]> {
		private static final long serialVersionUID = 1L;

		private transient Unpickler unpickler;
		private Seq<String> columnNames;

		public PickleRowWriter(Seq<String> columnNames) {
			this.columnNames = columnNames;
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
				Map<?, ?> row = unpickle(pickledRow);

				for (int idx = 0; idx < this.columnNames.length(); idx++) {
					Object value = row.get(this.columnNames.apply(idx));

					if (value == null) {
						throw new RuntimeException("Missing value for column " + this.columnNames.apply(idx));
					}

					buffer[idx] = value;
				}
			} catch (PickleException | IOException e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
		}

		private Map<?, ?> unpickle(byte[] pickledRow) throws IOException {
			Object unpickled = unpickler.loads(pickledRow);

			if (unpickled instanceof List) {
				List<?> list = (List<?>) unpickled;

				// TODO proper exceptions
				if (list.size() > 1) {
					throw new RuntimeException("Can't write a list of rows in one go ... must be a map!");
				}

				return (Map<?, ?>) list.get(0);
			} else {
				return (Map<?, ?>) unpickled;
			}
		}
	}
}