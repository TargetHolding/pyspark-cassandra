package pyspark_cassandra

import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.{ Row => DriverRow }
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.RowReader
import com.datastax.spark.connector.rdd.reader.RowReaderFactory

/** A container for a 'raw' row from the java driver, to be deserialized. */
case class UnreadRow(row: DriverRow, columnNames: Array[String], table: TableDef, protocolVersion: ProtocolVersion) {
  def deserialize(c: String) = {
    if (row.isNull(c)) null else Utils.deserialize(row.getColumnDefinitions.getType(c), row.getBytesUnsafe(c), protocolVersion)
  }

  def deserialize(c: Int) = {
    if (row.isNull(c)) null else Utils.deserialize(row.getColumnDefinitions.getType(c), row.getBytesUnsafe(c), protocolVersion)
  }
}

class DeferringRowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef])
    extends RowReader[UnreadRow] {

  def targetClass = classOf[UnreadRow]

  override def neededColumns: Option[Seq[ColumnRef]] = None // TODO or selected columns?

  override def read(row: DriverRow, columns: Array[String])(implicit protocol: ProtocolVersion) = {
    assert(row.getColumnDefinitions().size() >= columns.size, "Not enough columns available in row")
    UnreadRow(row, columns, table, protocol)
  }
}

class DeferringRowReaderFactory extends RowReaderFactory[UnreadRow] {
  def targetClass: Class[UnreadRow] = classOf[UnreadRow]

  def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[UnreadRow] = {
    new DeferringRowReader(table, selectedColumns)
  }
}

