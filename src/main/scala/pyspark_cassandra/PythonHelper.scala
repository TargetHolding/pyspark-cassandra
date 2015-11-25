package pyspark_cassandra

import java.util.List
import java.util.Map
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.AllColumns
import com.datastax.spark.connector.BytesInBatch
import com.datastax.spark.connector.ColumnName
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.datastax.spark.connector.streaming.toDStreamFunctions
import com.datastax.spark.connector.writer.BatchGroupingKey
import com.datastax.spark.connector.writer.TTLOption
import com.datastax.spark.connector.writer.TimestampOption
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.rdd.CassandraJoinRDD
import com.datastax.spark.connector.PartitionKeyColumns
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.rdd.CassandraJoinRDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.api.java.JavaDStream
import java.lang.Boolean

class PythonHelper() {
  Pickling.register()

  implicit def toPickleableRDD(rdd: RDD[_]) = new PicklableRDD(rdd)
  implicit def toUnpickleableRDD(rdd: RDD[Array[Byte]]) = new UnpicklableRDD(rdd)
  implicit def toUnpickleableStream(dstream: DStream[Array[Byte]]) = new UnpicklableDStream(dstream)

  def cassandraTable(jsc: JavaSparkContext, keyspace: String, table: String, readConf: Map[String, Any]): CassandraTableScanRDD[UnreadRow] =
    cassandraTable(jsc.sc, keyspace, table, readConf)

  def cassandraTable(sc: SparkContext, keyspace: String, table: String, readConf: Map[String, Any]) = {
    val conf = parseReadConf(sc, readConf)
    implicit var rrf = new DeferringRowReaderFactory()
    sc.cassandraTable(keyspace, table).withReadConf(conf)
  }

  def select(rdd: CassandraRDD[UnreadRow], columns: List[String]) = {
    rdd.select(columns.map { new ColumnName(_) }: _*)
  }

  def limit(rdd: CassandraRDD[UnreadRow], lim: Long) = {
    rdd.limit(lim)
  }

  def where(rdd: CassandraRDD[UnreadRow], cql: String, values: List[String]) = {
    rdd.where(cql, values: _*)
  }

  def cassandraCount(rdd: CassandraRDD[UnreadRow]) = rdd.cassandraCount()

  def pickleRows(rdd: CassandraRDD[UnreadRow]): RDD[Array[Byte]] = pickleRows(rdd, null)
  def pickleRows(rdd: CassandraRDD[UnreadRow], rowFormat: Integer): RDD[Array[Byte]] = pickleRows(rdd, rowFormat, false)
  def pickleRows(rdd: CassandraRDD[UnreadRow], rowFormat: Integer, keyed: Boolean) = {
    // TODO implement keying by arbitrary columns, analogues to the spanBy(...) and spark-cassandra-connector
    rdd.map(Format.parser(rowFormat, keyed)).pickle()
  }

  def pickleRows(rdd: CassandraJoinRDD[UnreadRow, UnreadRow]): RDD[Array[Byte]] = pickleRows(rdd, null)
  def pickleRows(rdd: CassandraJoinRDD[UnreadRow, UnreadRow], rowFormat: Integer): RDD[Array[Byte]] = pickleRows(rdd, rowFormat, false)
  def pickleRows(rdd: CassandraJoinRDD[UnreadRow, UnreadRow], rowFormat: Integer, keyed: Boolean) = {
    rdd.map(new JoinedRowTransformer()).pickle()
  }

  def javaRDD(rdd: RDD[_]) = JavaRDD.fromRDD(rdd)

  def spanBy(rdd: RDD[UnreadRow], columns: Array[String]) = {
    SpanBy.binary(rdd, columns)
  }

  def saveToCassandra(rdd: JavaRDD[Array[Byte]], keyspace: String, table: String, columns: Array[String],
    rowFormat: Integer, keyed: Boolean, writeConf: Map[String, Any]): Unit =
    saveToCassandra(rdd.rdd, keyspace, table, columns, rowFormat, keyed, writeConf)

  def saveToCassandra(rdd: RDD[Array[Byte]], keyspace: String, table: String, columns: Array[String],
    rowFormat: Integer, keyed: Boolean, writeConf: Map[String, Any]) = {

    val selectedColumns = columnSelector(columns)
    val conf = parseWriteConf(writeConf)

    implicit val rwf = new GenericRowWriterFactory(rowFormat, keyed)
    rdd.unpickle().saveToCassandra(keyspace, table, selectedColumns, conf)
  }

  def saveToCassandra(dstream: JavaDStream[Array[Byte]], keyspace: String, table: String, columns: Array[String],
    rowFormat: Integer, keyed: Boolean, writeConf: Map[String, Any]): Unit =
    saveToCassandra(dstream.dstream, keyspace, table, columns, rowFormat, keyed, writeConf)
    
  def saveToCassandra(dstream: DStream[Array[Byte]], keyspace: String, table: String, columns: Array[String],
    rowFormat: Integer, keyed: Boolean, writeConf: Map[String, Any]) = {

    val selectedColumns = columnSelector(columns)
    val conf = parseWriteConf(writeConf)

    implicit val rwf = new GenericRowWriterFactory(rowFormat, keyed)
    dstream.unpickle().saveToCassandra(keyspace, table, selectedColumns, conf)
  }

  def joinWithCassandraTable(rdd: JavaRDD[Array[Byte]], keyspace: String, table: String): CassandraJoinRDD[Any, UnreadRow] =
    joinWithCassandraTable(rdd.rdd, keyspace, table)

  def joinWithCassandraTable(rdd: RDD[Array[Byte]], keyspace: String, table: String) = {
    implicit val rwf = new GenericRowWriterFactory(null)
    implicit val rrf = new DeferringRowReaderFactory()
    rdd.unpickle().joinWithCassandraTable(keyspace, table)
  }

  def on(rdd: CassandraJoinRDD[Any, UnreadRow], columns: Array[String]) = {
    rdd.on(columnSelector(columns, PartitionKeyColumns))
  }

  def columnSelector(columns: Array[String], default: ColumnSelector = AllColumns) = {
    if (columns != null && columns.length > 0)
      SomeColumns(columns.map { ColumnName(_) }: _*)
    else
      default
  }

  private def parseReadConf(sc: SparkContext, readConf: Map[String, Any]) = {
    var conf = ReadConf.fromSparkConf(sc.getConf)

    if (readConf != null)
      for ((k, v) <- readConf) {
        (k, v) match {
          case ("split_count", v: Int) => conf = conf.copy(splitCount = Option(v))
          case ("split_size", v: Int) => conf = conf.copy(splitSizeInMB = v)
          case ("fetch_size", v: Int) => conf = conf.copy(fetchSizeInRows = v)
          case ("consistency_level", v: Int) => conf = conf.copy(consistencyLevel = ConsistencyLevel.values()(v))
          case ("consistency_level", v) => conf = conf.copy(consistencyLevel = ConsistencyLevel.valueOf(v.toString))
          case ("metrics_enabled", v: Boolean) => conf = conf.copy(taskMetricsEnabled = v)
          case _ => throw new IllegalArgumentException(s"Read conf key $k with value $v unsupported")
        }
      }

    conf
  }

  private def parseWriteConf(writeConf: Map[String, Any]) = {
    var conf = WriteConf()

    if (writeConf != null)
      for ((k, v) <- writeConf) {
        (k, v) match {
          case ("batch_size", v: Int) => conf = conf.copy(batchSize = BytesInBatch(v))
          case ("batch_buffer_size", v: Int) => conf = conf.copy(batchGroupingBufferSize = v)
          case ("batch_grouping_key", "replica_set") => conf = conf.copy(batchGroupingKey = BatchGroupingKey.ReplicaSet)
          case ("batch_grouping_key", "partition") => conf = conf.copy(batchGroupingKey = BatchGroupingKey.ReplicaSet)
          case ("consistency_level", v: Int) => conf = conf.copy(consistencyLevel = ConsistencyLevel.values()(v))
          case ("consistency_level", v) => conf = conf.copy(consistencyLevel = ConsistencyLevel.valueOf(v.toString))
          case ("parallelism_level", v: Int) => conf = conf.copy(parallelismLevel = v)
          case ("throughput_mibps", v: Number) => conf = conf.copy(throughputMiBPS = v.doubleValue())
          case ("ttl", v: Int) => conf = conf.copy(ttl = TTLOption.constant(v))
          case ("timestamp", v: Number) => conf = conf.copy(timestamp = TimestampOption.constant(v.longValue()))
          case ("metrics_enabled", v: Boolean) => conf = conf.copy(taskMetricsEnabled = v)
          case _ => throw new IllegalArgumentException(s"Write conf key $k with value $v unsupported")
        }
      }

    conf
  }
}

