package pyspark_cassandra;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

import org.apache.spark.sql.cassandra.CassandraSQLRow;

public class CassandraSQLRowPickler implements IObjectPickler {

	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		CassandraSQLRow row = (CassandraSQLRow) o;

		List<String> fieldNames = Types.toList(row.fieldNames());
		List<Object> fieldValues = Types.toList(row.fieldValues());

		out.write(Opcodes.GLOBAL);
		out.write("pyspark_cassandra.row\n_create_row\n".getBytes());
		out.write(Opcodes.MARK);
		pickler.save(fieldNames);
		pickler.save(fieldValues);
		out.write(Opcodes.TUPLE);
		out.write(Opcodes.REDUCE);
	}

}
