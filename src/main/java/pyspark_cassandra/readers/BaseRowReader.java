package pyspark_cassandra.readers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import pyspark_cassandra.types.Types;
import scala.Option;
import scala.collection.Seq;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.ColumnDef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;

public abstract class BaseRowReader<T> implements RowReader<T> {
	private static final long serialVersionUID = 1L;
	
	protected TableDef tbl;
	protected RowReaderOptions opts;

	public BaseRowReader(TableDef tbl, RowReaderOptions opts) {
		this.tbl = tbl;
		this.opts = opts;
	}

	@Override
	public Option<Object> requiredColumns() {
		return Option.apply(null);
	}

	@Override
	public Option<Object> consumedColumns() {
		return Option.apply(null);
	}

	@Override
	public Option<Seq<String>> columnNames() {
		return Option.apply(null);
	}

	protected Object readColumn(int idx, Row row, ProtocolVersion protocolVersion) {
		ByteBuffer bytes = row.getBytesUnsafe(idx);
		return bytes == null ? null : row.getColumnDefinitions().getType(idx).deserialize(bytes, protocolVersion);
	}

	protected Object readColumn(String name, Row row, ProtocolVersion protocolVersion) {
		ByteBuffer bytes = row.getBytesUnsafe(name);
		return bytes == null ? null : row.getColumnDefinitions().getType(name).deserialize(bytes, protocolVersion);
	}

	protected List<String> intersect(String[] columnNames, Seq<ColumnDef> def) {
		List<String> intersection = Types.toJavaList(columnNames);
		intersection.retainAll(columnNames(def));
		return intersection;
	}

	private List<String> columnNames(Seq<ColumnDef> def) {
		List<String> defs = new ArrayList<String>(def.size());

		for (int i = 0; i < def.size(); i++) {
			defs.add(def.apply(i).columnName());
		}

		return defs;
	}
}
