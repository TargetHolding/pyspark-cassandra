package pyspark_cassandra.readers;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import pyspark_cassandra.types.RawRow;
import pyspark_cassandra.types.Types;
import scala.collection.Seq;
import scala.Tuple2;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.ColumnDef;
import com.datastax.spark.connector.cql.TableDef;

public class PairReader implements Serializable, Function<Tuple2, Object > {
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Tuple2 pair) throws Exception {
		return parse(pair);
	}

	public Object parse(Tuple2 pair) {
		return pair._2;
	}
}
