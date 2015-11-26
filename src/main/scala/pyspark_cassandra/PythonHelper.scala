/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pyspark_cassandra

import pyspark_cassandra.Utils._

import java.lang.Boolean
import java.util.{ List, Map }

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext
import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming.toDStreamFunctions
import com.datastax.spark.connector.writer._

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
}
