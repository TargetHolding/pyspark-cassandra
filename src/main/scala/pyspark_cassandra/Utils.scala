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

import java.nio.ByteBuffer
import java.lang.{ Boolean => JBoolean }
import java.util.{ List => JList, Map => JMap }
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import org.apache.spark.SparkContext
import com.datastax.driver.core.{ CodecRegistry, ConsistencyLevel, DataType, ProtocolVersion }
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.writer._

object Utils {
  def columnSelector(columns: Array[String], default: ColumnSelector = AllColumns) = {
    if (columns != null && columns.length > 0) {
      SomeColumns(columns.map { ColumnName(_) }: _*)
    } else {
      default
    }
  }

  def parseReadConf(sc: SparkContext, readConf: Option[JMap[String, Any]]) = {
    var conf = ReadConf.fromSparkConf(sc.getConf)

    readConf match {
      case Some(rc) =>
        for { (k, v) <- rc } {
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
      case None => // do nothing
    }

    conf
  }

  def parseWriteConf(writeConf: Option[JMap[String, Any]]) = {
    var conf = WriteConf()

    writeConf match {
      case Some(wc) =>
        for { (k, v) <- wc } {
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
      case None => // do nothing
    }

    conf
  }
}

object Format extends Enumeration {
  val DICT, TUPLE, ROW = Value

  def apply(format: Integer): Option[Format.Value] =
    if (format != null) Some(Format(format)) else None

  def parser(example: Any): FromUnreadRow[_] = {
    val format = detect(example)
    return Format.parser(format._1, format._2)
  }

  def parser(format: Option[Format.Value], keyed: Option[Boolean]): FromUnreadRow[_] =
    parser(format.getOrElse(Format.ROW), keyed.getOrElse(false))

  def parser(format: Format.Value, keyed: Boolean): FromUnreadRow[_] = {
    (format, keyed) match {
      case (Format.ROW, false) => ToRow
      case (Format.ROW, true) => ToKVRows

      case (Format.TUPLE, false) => ToTuple
      case (Format.TUPLE, true) => ToKVTuple

      case (Format.DICT, false) => ToDict
      case (Format.DICT, true) => ToKVDicts

      case _ => throw new IllegalArgumentException()
    }
  }

  def detect(row: Any) = {
    // The detection works because primary keys can't be maps, sets or lists. If the detection still fails, a
    // user must set the row_format explicitly

    row match {
      // Rows map to ROW of course
      case row: Row => (Format.ROW, false)

      // If the row is a map, the only possible format is DICT
      case row: Map[_, _] => (Format.DICT, false)
      case row: JMap[_, _] => (Format.DICT, false)

      // otherwise it must be a tuple
      case row: Array[_] =>
        // If the row is a tuple of length two, try to figure out if it's a (key,value) tuple
        if (row.length == 2) {
          val Array(k, v) = row

          k match {
            case k: Map[_, _] => (Format.DICT, true)
            case k: Array[_] => (Format.TUPLE, true)
            case k: Row => (Format.ROW, true)
            case _ => (Format.TUPLE, false)
          }
        }

        (Format.TUPLE, false)

      // or if we really can't figure it out, request the user to set it explicitly
      case _ =>
        val cls = row.getClass()
        throw new RuntimeException(s"Unable to detect or unsupported row format ($cls). Set it explicitly " +
          "with saveToCassandra(..., row_format=...).")
    }
  }
}
