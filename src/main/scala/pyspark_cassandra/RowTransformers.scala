package pyspark_cassandra

trait FromUnreadRow[T] extends (UnreadRow => T) with Serializable

// TODO consider replacying array of Map[String, Object] with a real tuple
// not just here by the way, but all over the place ... this is Scala!
trait ToKV[KV] extends FromUnreadRow[Array[Any]] {
  def apply(row: UnreadRow): Array[Any] = {
    val k = transform(row, row.columnNames.intersect(row.table.primaryKey.map { _.columnName }))
    val v = transform(row, row.columnNames.intersect(row.table.regularColumns.map { _.columnName }))
    return Array(k, v)
  }

  def transform(row: UnreadRow, columns: Array[String]): KV
}

// TODO why ship field names for every row?
case class Row(fields: Array[String], values: Array[AnyRef])

object ToRow extends FromUnreadRow[Row] {
  override def apply(row: UnreadRow): Row = {
    Row(row.columnNames, row.columnNames.map { c => row.deserialize(c) })
  }
}

object ToKVRows extends ToKV[Row] {
  def transform(row: UnreadRow, columns: Array[String]): Row = {
    Row(columns, columns.map { c => row.deserialize(c) })
  }
}

object ToTuple extends FromUnreadRow[Array[Any]] {
  def apply(row: UnreadRow): Array[Any] = {
    (row.columnNames.indices map { c => row.deserialize(c) }).toArray
  }
}

object ToKVTuple extends ToKV[Array[Any]] {
  def transform(row: UnreadRow, columns: Array[String]): Array[Any] = {
    columns.map { c => row.deserialize(c) }
  }
}

object ToDict extends FromUnreadRow[Map[String, Object]] {
  def apply(row: UnreadRow): Map[String, Object] = {
    Map(row.columnNames.zipWithIndex.map { case (c, i) => c -> row.deserialize(i) }: _*)
  }
}

object ToKVDicts extends ToKV[Map[String, Object]] {
  def transform(row: UnreadRow, columns: Array[String]): Map[String, Object] = {
    Map(columns.map { c => c -> row.deserialize(c) }: _*)
  }
}

class JoinedRowTransformer extends (((Any, UnreadRow)) => (Any, Any)) with Serializable {
  def apply(pair: (Any, UnreadRow)): (Any, Any) = {
    val format = Format.detect(pair._1)
    val parser = Format.parser(format._1, format._2)
    return (pair._1, parser.apply(pair._2))
  }
}
