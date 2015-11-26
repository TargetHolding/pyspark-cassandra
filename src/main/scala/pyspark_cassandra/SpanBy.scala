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

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD

import com.datastax.driver.core.{ DataType, ProtocolVersion }
import com.datastax.spark.connector.toRDDFunctions

case class DataFrame(names: Array[String], types: Array[String], values: Seq[ArrayBuffer[Any]])

object SpanBy {
  def binary(rdd: RDD[UnreadRow], columns: Array[String]) = {
    // span by the given columns
    val spanned = rdd.spanBy { r => columns.map { c => r.row.getBytesUnsafe(c) } }

    // TODO what about the option to just return rows spanned by key?

    // deserialize the spans
    spanned.map {
      case (k, rows) => {
        // peak at the first row to get the protocol version
        val pv = rows.head.protocolVersion

        // get the columns for the data frame (so excluding the ones spanned by)
        val colDefs = rows.head.row.getColumnDefinitions.asList()
        val colTypesWithIdx = colDefs.map {
          d => d.getType
        }.zipWithIndex.filter {
          case (c, i) => columns.contains(c.getName)
        }

        // deserialize the spanning key
        val deserializedKey = k.zipWithIndex.map {
          case (bb, i) => Utils.deserialize(colDefs.get(i).getType, bb, pv)
        }

        // transpose the rows in to columns and 'deserialize'
        val df = colDefs.map { x => new ArrayBuffer[Any] }
        for (row <- rows; (ct, i) <- colTypesWithIdx) {
          df(i) += row.deserialize(i)
        }

        // list the numpy types of the columns in the span (i.e. the non-key columns)
        val numpyTypes = colTypesWithIdx.map { case (c, i) => numpyType(c).getOrElse(null) }

        // return the key and 'dataframe container'
        (deserializedKey, new DataFrame(columns, numpyTypes.toArray, df))
      }
    }
  }

  /**
   * 'Deserializes' a value of the given type _only if_ there is no binary representation possibly which can
   * be converted into a numpy array. I.e. longs will _not_ actually be deserialized, but Strings or UUIDs
   * will. If possible the value will be written out as a binary string for an entire column to be converted
   * to Numpy arrays.
   */
  private def deserialize(dataType: DataType, bytes: ByteBuffer, protocolVersion: ProtocolVersion) = {
    if (binarySupport(dataType)) bytes else Utils.deserialize(dataType, bytes, protocolVersion)
  }

  /** Checks if a Cassandra type can be represented as a binary string. */
  private def binarySupport(dataType: DataType) = {
    numpyType(dataType) match {
      case Some(x) => true
      case None => false
    }
  }

  /** Provides a Numpy type string for every Cassandra type supported. */
  private def numpyType(dataType: DataType) = {
    Option(dataType.getName match {
      case DataType.Name.BOOLEAN => ">b1"
      case DataType.Name.INT => ">i4"
      case DataType.Name.BIGINT => ">i8"
      case DataType.Name.COUNTER => ">i8"
      case DataType.Name.FLOAT => ">f4"
      case DataType.Name.DOUBLE => ">f8"
      case DataType.Name.TIMESTAMP => ">M8[ms]"
      case _ => null
    })
  }
}
