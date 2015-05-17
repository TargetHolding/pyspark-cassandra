package pyspark_cassandra.readers;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import pyspark_cassandra.types.RawRow;
import pyspark_cassandra.types.Types;
import scala.collection.Seq;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.ColumnDef;
import com.datastax.spark.connector.cql.TableDef;

public abstract class RowReader<T> implements Serializable, Function<RawRow, T> {
	private static final long serialVersionUID = 1L;

	@Override
	public T call(RawRow row) throws Exception {
		return parse(row.getRow(), row.getColumnNames(), row.getTableDef(), row.getProtocolVersion());
	}

	public abstract T parse(Row row,
			String[] columnNames,
			TableDef tableDef,
			ProtocolVersion protocolVersion);

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
