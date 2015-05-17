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

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import pyspark_cassandra.pickling.BatchPickle;
import pyspark_cassandra.pickling.BatchUnpickle;
import pyspark_cassandra.readers.DeferringRowReaderFactory;
import pyspark_cassandra.readers.DictRowReader;
import pyspark_cassandra.readers.KVDictsRowReader;
import pyspark_cassandra.readers.KVRowsReaderFactory;
import pyspark_cassandra.readers.KVTuplesRowReader;
import pyspark_cassandra.readers.LWRowReader;
import pyspark_cassandra.readers.RowReader;
import pyspark_cassandra.readers.TupleRowReader;
import pyspark_cassandra.types.RawRow;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.datastax.spark.connector.japi.DStreamJavaFunctions;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions;
import com.datastax.spark.connector.japi.RDDJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.rdd.ReadConf;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;

/**
 * Main access point for Cassandra related features from PySpark. Many of the features in pyspark_cassandra are mainly
 * implemented in Java and are only accessed from python.
 */
public class PythonHelper {
	public CassandraJavaRDD<RawRow> cassandraTable(String keyspace, String table, JavaSparkContext sc,
			Map<String, Object> readConf) {
		return cassandraTable(keyspace, table, sc, readConf, new DeferringRowReaderFactory());
	}

	private <T> CassandraJavaRDD<T> cassandraTable(String keyspace, String table, JavaSparkContext sc,
			Map<String, Object> readConf, RowReaderFactory<T> rrf) {
		return CassandraJavaUtil.javaFunctions(sc)
				.cassandraTable(keyspace, table, rrf)
				.withReadConf(readConf(readConf));
	}

	public JavaRDD<byte[]> spanBy(CassandraJavaRDD<RawRow> rdd, String[] columns) {
		return rdd
				.mapPartitions(new SpanningDataFrameReader(columns), true)
				.mapPartitions(new BatchPickle<Object[]>(), true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public JavaRDD<byte[]> parseRows(CassandraJavaRDD<RawRow> rdd, Integer rowFormat) {
		return rdd
				.map(rowParser(rowFormat))
				.mapPartitions(new BatchPickle(), true);
	}

	@SuppressWarnings("rawtypes")
	private RowReader rowParser(Integer rowFormat) {
		if (rowFormat == null) {
			return new LWRowReader();
		}

		switch (RowFormat.values()[rowFormat]) {
		case ROW:
			return new LWRowReader();
		case TUPLE:
			return new TupleRowReader();
		case DICT:
			return new DictRowReader();
		case KV_TUPLES:
			return new KVTuplesRowReader();
		case KV_DICTS:
			return new KVDictsRowReader();
		case KV_ROWS:
			return new KVRowsReaderFactory();
		default:
			throw new IllegalArgumentException();
		}
	}

	public void saveToCassandra(JavaRDD<byte[]> rdd, String keyspace, String table, String[] columns,
			Map<String, Object> writeConf, Integer rowFormat) {
		RDDJavaFunctions<Object> crdd = CassandraJavaUtil.javaFunctions(rdd.flatMap(new BatchUnpickle()));
		ObjectRowWriterFactory rrf = rowWriterFactory(rowFormat);
		RDDAndDStreamCommonJavaFunctions<Object>.WriterBuilder builder = crdd.writerBuilder(keyspace, table, rrf);

		saveToCassandra(builder, columns, writeConf);
	}

	public void saveToCassandra(JavaDStream<byte[]> dstream, String keyspace, String table, String[] columns,
			Map<String, Object> writeConf, Integer rowFormat) {
		DStreamJavaFunctions<Object> cdstream = CassandraStreamingJavaUtil.javaFunctions(dstream.flatMap(new BatchUnpickle()));
		ObjectRowWriterFactory rrf = rowWriterFactory(rowFormat);
		RDDAndDStreamCommonJavaFunctions<Object>.WriterBuilder builder = cdstream.writerBuilder(keyspace, table, rrf);

		saveToCassandra(builder, columns, writeConf);
	}

	private void saveToCassandra(RDDAndDStreamCommonJavaFunctions<Object>.WriterBuilder builder, String[] columns,
			Map<String, Object> writeConf) {
		// Defaults to false. This hides some compatibility issues with default settings.
		builder = builder.withTaskMetricsEnabled(false);

		if (writeConf != null) {
			builder = writeConf(builder, writeConf);
		}

		if (columns != null && columns.length > 0) {
			builder = builder.withColumnSelector(CassandraJavaUtil.someColumns(columns));
		}

		builder.saveToCassandra();
	}

	private ObjectRowWriterFactory rowWriterFactory(Integer rowFormat) {
		if (rowFormat == null) {
			return new ObjectRowWriterFactory(null);
		} else {
			return new ObjectRowWriterFactory(RowFormat.values()[rowFormat]);
		}
	}

	// TODO some read conf options may also be set in the config of the spark context?
	private ReadConf readConf(Map<String, Object> values) {
		int splitSize = (int) get(values, "split_size", ReadConf.DefaultSplitSize());
		int fetchSize = (int) get(values, "fetch_size", ReadConf.DefaultFetchSize());

		ConsistencyLevel consistencyLevel = getConsistencyLevel(get(values, "consistency_level", null),
				ReadConf.DefaultConsistencyLevel());

		// Defaults to false if not set. This hides some compatibility issues with default settings
		boolean taskMetricsEnabled = (boolean) get(values, "metrics_enabled", false);

		return new ReadConf(splitSize, fetchSize, consistencyLevel, taskMetricsEnabled);
	}

	private RDDAndDStreamCommonJavaFunctions<Object>.WriterBuilder writeConf(
			RDDAndDStreamCommonJavaFunctions<Object>.WriterBuilder builder, Map<String, Object> values) {

		if (values.containsKey("batch_size")) {
			builder = builder.withBatchSize(CassandraJavaUtil.bytesInBatch((int) values.get("batch_size")));
		}

		if (values.containsKey("batch_buffer_size")) {
			builder = builder.withBatchGroupingBufferSize((int) values.get("batch_buffer_size"));
		}

		if (values.containsKey("batch_grouping_key")) {
			String string = (String) values.get("batch_grouping_key");

			switch (string) {
			case "replica_set":
				builder = builder.withBatchGroupingKey(CassandraJavaUtil.BATCH_GROUPING_KEY_REPLICA_SET);
				break;
			case "partition":
				builder = builder.withBatchGroupingKey(CassandraJavaUtil.BATCH_GROUPING_KEY_PARTITION);
				break;
			}
		}

		if (values.containsKey("consistency_level")) {
			builder = builder.withConsistencyLevel(getConsistencyLevel(values.get("consistency_level")));
		}

		if (values.containsKey("parallelism_level")) {
			builder = builder.withParallelismLevel((int) values.get("parallelism_level"));
		}

		if (values.containsKey("throughput_mibps")) {
			builder = builder.withThroughputMBPS((int) values.get("throughput_mibps"));
		}

		if (values.containsKey("ttl")) {
			builder = builder.withConstantTTL((int) values.get("ttl"));
		}

		if (values.containsKey("timestamp")) {
			builder = builder.withConstantTimestamp((long) values.get("timestamp"));
		}

		if (values.containsKey("metrics_enabled")) {
			builder = builder.withTaskMetricsEnabled(((Boolean) values.get("metrics_enabled")) == true);
		}

		return builder;
	}

	private ConsistencyLevel getConsistencyLevel(Object v) {
		return this.getConsistencyLevel(v, null);
	}

	private ConsistencyLevel getConsistencyLevel(Object v, ConsistencyLevel def) {
		if (v == null) {
			return null;
		} else if (v instanceof Integer) {
			return ConsistencyLevel.values()[(int) v];
		} else {
			return ConsistencyLevel.valueOf(v.toString());
		}
	}

	private <K, V> V get(Map<K, V> m, K k, V def) {
		if (m == null) {
			return def;
		}

		V v = m.get(k);

		if (v == null) {
			return def;
		} else {
			return v;
		}
	}
}
