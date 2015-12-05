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
import pyspark_util.Pickling._
import pyspark_util.Conversions._

import java.lang.Boolean
import java.util.{ List => JList, Map => JMap }

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

  /* ----------------------------------------------------------------------- */
  /* loading from cassandra ------------------------------------------------ */
  /* ----------------------------------------------------------------------- */

  def cassandraTable(jsc: JavaSparkContext, keyspace: String, table: String, readConf: JMap[String, Any]) = {
    val conf = parseReadConf(jsc.sc, Some(readConf))
    implicit val rrf = new DeferringRowReaderFactory()
    jsc.sc.cassandraTable(keyspace, table).withReadConf(conf)
  }

  def select(rdd: CassandraRDD[UnreadRow], columns: Array[String]) = {
    rdd.select(columns.map { new ColumnName(_) }: _*)
  }

  def limit(rdd: CassandraRDD[UnreadRow], lim: Long) = {
    rdd.limit(lim)
  }

  def where(rdd: CassandraRDD[UnreadRow], cql: String, values: Array[Any]) = {
    rdd.where(cql, values: _*)
  }

  def cassandraCount(rdd: CassandraRDD[UnreadRow]) = rdd.cassandraCount()

  /* ----------------------------------------------------------------------- */
  /* span by columns ------------------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  def spanBy(rdd: RDD[UnreadRow], columns: Array[String]) = {
    SpanBy.binary(rdd, columns)
  }

  /* ----------------------------------------------------------------------- */
  /* save to cassandra ----------------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  /* rdds ------------------------------------------------------------------ */

  def saveToCassandra(rdd: JavaRDD[Array[Byte]], keyspace: String, table: String, columns: Array[String],
    rowFormat: Integer, keyed: Boolean, writeConf: JMap[String, Any]) = {

    val selectedColumns = columnSelector(columns)
    val conf = parseWriteConf(Some(writeConf))

    implicit val rwf = new GenericRowWriterFactory(Format(rowFormat), asBooleanOption(keyed))
    rdd.rdd.unpickle().saveToCassandra(keyspace, table, selectedColumns, conf)
  }

  /* dstreams -------------------------------------------------------------- */

  def saveToCassandra(dstream: JavaDStream[Array[Byte]], keyspace: String, table: String, columns: Array[String],
    rowFormat: Integer, keyed: Boolean, writeConf: JMap[String, Any]) = {

    val selectedColumns = columnSelector(columns)
    val conf = parseWriteConf(Some(writeConf))

    implicit val rwf = new GenericRowWriterFactory(Format(rowFormat), asBooleanOption(keyed))
    dstream.dstream.unpickle().saveToCassandra(keyspace, table, selectedColumns, conf)
  }

  /* ----------------------------------------------------------------------- */
  /* join with cassandra tables -------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  /* rdds ------------------------------------------------------------------ */

  def joinWithCassandraTable(rdd: JavaRDD[Array[Byte]], keyspace: String, table: String): CassandraJoinRDD[Any, UnreadRow] = {
    implicit val rwf = new GenericRowWriterFactory(None, None)
    implicit val rrf = new DeferringRowReaderFactory()
    rdd.rdd.unpickle().joinWithCassandraTable(keyspace, table)
  }

  def on(rdd: CassandraJoinRDD[Any, UnreadRow], columns: Array[String]) = {
    rdd.on(columnSelector(columns, PartitionKeyColumns))
  }

  /* dstreams -------------------------------------------------------------- */

  def joinWithCassandraTable(dstream: JavaDStream[Array[Byte]], keyspace: String, table: String,
    selectedColumns: Array[String], joinColumns: Array[String]): DStream[(Any, UnreadRow)] = {
    val columns = columnSelector(selectedColumns)
    val joinOn = columnSelector(joinColumns, PartitionKeyColumns)
    implicit val rwf = new GenericRowWriterFactory(None, None)
    implicit val rrf = new DeferringRowReaderFactory()
    dstream.dstream.unpickle().joinWithCassandraTable(keyspace, table, columns, joinOn)
  }

  /* ----------------------------------------------------------------------- */
  /* utilities for moving rdds and dstreams from and to pyspark ------------ */
  /* ----------------------------------------------------------------------- */

  def pickleRows(rdd: CassandraRDD[UnreadRow]): RDD[Array[Byte]] =
    pickleRows(rdd, null)

  def pickleRows(rdd: CassandraRDD[UnreadRow], rowFormat: Integer): RDD[Array[Byte]] =
    pickleRows(rdd, rowFormat, false)

  def pickleRows(rdd: CassandraRDD[UnreadRow], rowFormat: Integer, keyed: Boolean) = {
    // TODO implement keying by arbitrary columns, analogues to the spanBy(...) and spark-cassandra-connector
    val parser = Format.parser(Format(rowFormat), asBooleanOption(keyed))
    rdd.map(parser).pickle()
  }

  def pickleRows(rdd: CassandraJoinRDD[Any, UnreadRow], rowFormat: Integer, keyed: Boolean): RDD[Array[Byte]] =
    pickleRows(rdd)

  def pickleRows(rdd: CassandraJoinRDD[Any, UnreadRow], rowFormat: Integer): RDD[Array[Byte]] =
    pickleRows(rdd)

  def pickleRows(rdd: CassandraJoinRDD[Any, UnreadRow]) =
    rdd.map(new JoinedRowTransformer()).pickle()

  def pickleRows(dstream: DStream[(Any, UnreadRow)]) = {
    dstream.map(new JoinedRowTransformer()).transform((rdd, time) => rdd.pickle())
  }

  def javaRDD(rdd: RDD[_]) = JavaRDD.fromRDD(rdd)

  def javaDStream(dstream: DStream[_]) = JavaDStream.fromDStream(dstream)
}
