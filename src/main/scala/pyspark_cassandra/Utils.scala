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
import java.util.{ Map => JMap }

import com.datastax.driver.core.DataType
import com.datastax.driver.core.ProtocolVersion

object Utils {
  def deserialize(dt: DataType, b: ByteBuffer, pv: ProtocolVersion) = dt.deserialize(b, pv)
}

// when moving tospark cassandra connector 1.5.x :

/*
import com.datastax.driver.core.CodecRegistry
object Utils {
  def deserialize(dt: DataType, b: ByteBuffer, pv: ProtocolVersion) = codec(dt).deserialize(b, pv)
  def codec(dt: DataType) = CodecRegistry.DEFAULT_INSTANCE.codecFor(dt)
}
*/

object Format extends Enumeration {
  val DICT, TUPLE, ROW = Value

  def parser(format: Integer): FromUnreadRow[_] = parser(format, false)
  def parser(format: Format.Value): FromUnreadRow[_] = parser(format, false)

  def parser(format: Integer, keyed: Boolean): FromUnreadRow[_] = {
    val fmt = if (format == null)
      Format.ROW
    else
      Format(format)

    parser(fmt, keyed)
  }

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
