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

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import pyspark_cassandra.types.RawRow;

public class SpanningDataFrameReader implements FlatMapFunction<Iterator<RawRow>, Object[]> {
	private static final long serialVersionUID = 1L;

	private String[] spanBy;

	public SpanningDataFrameReader(String[] spanBy) {
		this.spanBy = spanBy;
	}

	@Override
	public Iterable<Object[]> call(final Iterator<RawRow> part) throws Exception {
		if (!part.hasNext()) {
			return EmptyIterable.get();
		}

		return new SpanningIterator(part, spanBy);
	}
}