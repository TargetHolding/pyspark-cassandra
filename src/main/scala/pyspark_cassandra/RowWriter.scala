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

import java.util.{ Map => JMap }

import scala.collection.{ IndexedSeq, Seq }

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.writer.{ RowWriter, RowWriterFactory }

class GenericRowWriterFactory(format: Option[Format.Value], keyed: Option[Boolean]) extends RowWriterFactory[Any] {
  def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowWriter[Any] = {
    new GenericRowWriter(format, keyed, selectedColumns)
  }
}

class GenericRowWriter(format: Option[Format.Value], keyed: Option[Boolean], columns: IndexedSeq[ColumnRef]) extends RowWriter[Any] {
  val cNames = columns.map { _.columnName }
  val idxedCols = cNames.zipWithIndex

  def columnNames: Seq[String] = cNames
  def indexedColumns = idxedCols

  var fmt: Option[(Format.Value, Boolean)] = None

  def readColumnValues(row: Any, buffer: Array[Any]): Unit = {
    if (fmt.isEmpty) {
      fmt = Some((format, keyed) match {
        case (Some(x: Format.Value), Some(y: Boolean)) => (x, y)
        case _ => Format.detect(row)
      })
    }

    fmt.get match {
      case (Format.TUPLE, false) => readAsTuple(row, buffer)
      case (Format.TUPLE, true) => readAsKVTuples(row, buffer)
      case (Format.DICT, false) => readAsDict(row, buffer)
      case (Format.DICT, true) => readAsKVDicts(row, buffer)
      case (Format.ROW, false) => readAsRow(row, buffer)
      case (Format.ROW, true) => readAsKVRows(row, buffer)
      case _ => throw new IllegalArgumentException("Unsupported or unknown cassandra row format")
    }
  }

  def readAsTuple(row: Any, buffer: Array[Any]) = {
    val v = row.asInstanceOf[Array[Any]]
    System.arraycopy(v, 0, buffer, 0, Math.min(v.length, buffer.length));
  }

  def readAsKVTuples(row: Any, buffer: Array[Any]) = {
    val k, v = row.asInstanceOf[Array[Array[Any]]]

    val keyLength = Math.min(k.length, buffer.length);
    System.arraycopy(k, 0, buffer, 0, keyLength);
    System.arraycopy(v, 0, buffer, keyLength, Math.min(v.length, buffer.length - keyLength));
  }

  def readAsDict(row: Any, buffer: Array[Any]) = {
    val v = row.asInstanceOf[JMap[Any, Any]]

    indexedColumns.map {
      case (s, i) => buffer(i) = v.get(s)
    }
  }

  def readAsKVDicts(row: Any, buffer: Array[Any]) = {
    val Array(k, v) = row.asInstanceOf[Array[JMap[Any, Any]]]

    indexedColumns.map {
      case (c, i) => {
        buffer(i) = if (v.containsKey(c)) v.get(c) else k.get(c)
      }
    }
  }

  def readAsRow(row: Any, buffer: Array[Any]): Unit = {
    val v = row.asInstanceOf[Row]
    readAsRow(v, buffer, 0)
  }

  def readAsKVRows(row: Any, buffer: Array[Any]) = {
    val Array(k, v) = row.asInstanceOf[Array[Row]]
    readAsRow(k, buffer, 0)
    readAsRow(v, buffer, k.fields.length)
  }

  private def readAsRow(row: Row, buffer: Array[Any], offset: Int): Unit = {
    row.fields.zipWithIndex.foreach {
      case (f, srcIdx) =>
        val dstIdx = columnNames.indexOf(f)
        if (dstIdx >= 0) {
          buffer(dstIdx + offset) = row.values(srcIdx)
        }
    }
  }
}
