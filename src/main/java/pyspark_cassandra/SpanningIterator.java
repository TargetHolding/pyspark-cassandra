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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import pyspark_cassandra.types.LWRow;
import pyspark_cassandra.types.DataFrame;
import pyspark_cassandra.types.RawRow;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public final class SpanningIterator implements Iterable<Object[]> {
	private PeekingIterator<RawRow> part;
	private String[] spanBy;

	private ColumnDefinitions colDefs;
	private ProtocolVersion pv;
	private String[] dfColumns;
	private DataType[] dfColumnTypes;

	public SpanningIterator(Iterator<RawRow> part, String[] spanBy) {
		this.part = Iterators.peekingIterator(part);
		this.spanBy = spanBy;

		RawRow first = this.part.peek();
		colDefs = first.getRow().getColumnDefinitions();
		pv = first.getProtocolVersion();
		dfColumns = subtract(first.getColumnNames(), spanBy);
		dfColumnTypes = getTypes(dfColumns, colDefs);
	}

	@Override
	public Iterator<Object[]> iterator() {
		return new Iterator<Object[]>() {
			private ByteBuffer[] key = null;
			private List<ByteBuffer[]> df = null;

			@Override
			public boolean hasNext() {
				return part.hasNext();
			}

			@Override
			public Object[] next() {
				// TODO transpose is done in packSpan, but can already be done here
				
				while (part.hasNext()) {
					Row row = part.next().getRow();

					if (key == null) {
						key = buffers(row, spanBy);
						df = new ArrayList<ByteBuffer[]>();
					} else if (!sameKey(row, key)) {
						try {
							return packSpan(key, df);
						} finally {
							key = buffers(row, spanBy);
							df = new ArrayList<ByteBuffer[]>();
							df.add(buffers(row, dfColumns));
						}
					}

					df.add(buffers(row, dfColumns));
				}

				return packSpan(key, df);
			}

			private Object[] packSpan(ByteBuffer[] key, List<ByteBuffer[]> rows) {
				Object[] deserializedKey = deserializeSpanningKey(key);
				DataFrame dataframe = deserialize(rows);

				return new Object[] { new LWRow(spanBy, deserializedKey), dataframe };
			}

			private DataFrame deserialize(List<ByteBuffer[]> rows) {
				int clen = dfColumns.length;

				@SuppressWarnings("unchecked")
				List<Object>[] values = new List[clen];
				String[] types = new String[clen];

				for (int c = 0; c < clen; c++) {
					values[c] = new ArrayList<Object>();
					types[c] = numpyType(dfColumnTypes[c]);
				}

				for (ByteBuffer[] row : rows) {
					for (int c = 0; c < clen; c++) {
						values[c].add(deserialize(dfColumnTypes[c], row[c]));
					}
				}

				return new DataFrame(dfColumns, types, values);
			}

			private Object[] deserializeSpanningKey(ByteBuffer[] buffers) {
				Object[] deserialized = new Object[buffers.length];

				for (int i = 0; i < spanBy.length; i++) {
					deserialized[i] = colDefs.getType(spanBy[i]).deserialize(buffers[i], pv);
				}

				return deserialized;
			}

			/**
			 * 'Deserializes' a value of the given type _only if_ there is no binary representation possibly which can
			 * be converted into a numpy array. I.e. longs will _not_ actually be deserialized, but Strings or UUIDs
			 * will. If possible the value will be written out as a binary string for an entire column to be converted
			 * to Numpy arrays.
			 */
			private Object deserialize(DataType type, ByteBuffer value) {
				if (binarySupported(type)) {
					return value;
				} else {
					return type.deserialize(value, pv);
				}
			}

			/**
			 * Checks if a Cassandra type can be represented as a binary string.
			 */
			private boolean binarySupported(DataType ctype) {
				return numpyType(ctype) != null;
			}

			/**
			 * Provides a Numpy type string for every Cassandra type supported.
			 */
			private String numpyType(DataType dataType) {
				switch (dataType.getName()) {
				case BOOLEAN:
					return ">b1";
				case INT:
					return ">i4";
				case BIGINT:
				case COUNTER:
					return ">i8";
				case FLOAT:
					return ">f4";
				case DOUBLE:
					return ">f8";
				case TIMESTAMP:
					return ">M8[ms]";
				default:
					return null;
				}
			}

			/**
			 * Takes columns from a row as ByteBuffers.
			 */
			private ByteBuffer[] buffers(Row row, String[] columns) {
				ByteBuffer[] key = new ByteBuffer[columns.length];

				for (int i = 0; i < columns.length; i++) {
					key[i] = row.getBytesUnsafe(columns[i]);
				}

				return key;
			}

			/**
			 * Checks if the values in the row are binary equal to the ByteBuffers in the given key.
			 */
			private boolean sameKey(Row row, ByteBuffer[] key) {
				for (int i = 0; i < key.length; i++) {
					if (!key[i].equals(row.getBytesUnsafe(spanBy[i]))) {
						return false;
					}
				}

				return true;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/**
	 * Subtracts elements from an array which are in another array with the assumption that both arrays contain unique
	 * elements and all elements which are to be subtracted are in the array to remove them from.
	 * 
	 * @param from
	 *            The array to subtract from.
	 * @param what
	 *            The array containing the elements to remove from the first array.
	 * @return A new array with only the elements which occurred in the first array and not in the other with a length
	 *         equal to the length of the first array minus the length of the second array.
	 */
	private <T> T[] subtract(T[] from, T[] what) {
		T[] sorted = Arrays.copyOf(what, what.length);
		Arrays.sort(sorted);

		@SuppressWarnings("unchecked")
		T[] diff = (T[]) Array.newInstance(from.getClass().getComponentType(), from.length - what.length);

		for (int i = 0, j = 0; i < from.length; i++) {
			if (Arrays.binarySearch(sorted, from[i]) < 0) {
				diff[j++] = from[i];
			}
		}

		return diff;
	}

	/**
	 * Takes the types from the given column definitions for each name in the given names.
	 */
	private DataType[] getTypes(String[] names, ColumnDefinitions defs) {
		DataType[] types = new DataType[names.length];

		for (int c = 0; c < names.length; c++) {
			types[c] = defs.getType(names[c]);
		}

		return types;
	}
}