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

import java.util.Map
import java.lang.Boolean

import scala.collection.IndexedSeq
import scala.collection.Seq

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.writer.RowWriter
import com.datastax.spark.connector.writer.RowWriterFactory

class GenericRowWriterFactory(format: Integer, keyed: Boolean = false) extends RowWriterFactory[Any] {
  def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowWriter[Any] = {
    new GenericRowWriter(format, keyed, selectedColumns)
  }
}

class GenericRowWriter(format: Integer, keyed: Boolean, columns: IndexedSeq[ColumnRef]) extends RowWriter[Any] {
  def columnNames: Seq[String] = columns.map { _.columnName }
  def indexedColumns = columnNames.zipWithIndex

  def readColumnValues(row: Any, buffer: Array[Any]): Unit = {
    val fmt = (
      if (format == null) Format.detect(row)
      else (Format(format), if (keyed != null) keyed else false))

    fmt match {
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
    val v = row.asInstanceOf[Map[Any, Any]]

    indexedColumns.map {
      case (s, i) => buffer(i) = v.get(s)
    }
  }

  def readAsKVDicts(row: Any, buffer: Array[Any]) = {
    val Array(k, v) = row.asInstanceOf[Array[Map[Any, Any]]]

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
        if (dstIdx >= 0)
          buffer(dstIdx + offset) = row.values(srcIdx)
    }
  }
}
