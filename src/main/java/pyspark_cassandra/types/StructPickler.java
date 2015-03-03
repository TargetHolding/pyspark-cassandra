package pyspark_cassandra.types;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

import scala.collection.IndexedSeq;

public abstract class StructPickler implements IObjectPickler {

	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		List<String> fieldNames = Types.toJavaList(this.getFieldNames(o));
		List<Object> fieldValues = Types.toJavaList(this.getFieldValues(o));

		out.write(Opcodes.GLOBAL);
		out.write(this.getCreator().getBytes());
		out.write(Opcodes.MARK);
		pickler.save(fieldNames);
		pickler.save(fieldValues);
		out.write(Opcodes.TUPLE);
		out.write(Opcodes.REDUCE);
	}

	public abstract IndexedSeq<String> getFieldNames(Object o);

	public abstract IndexedSeq<Object> getFieldValues(Object o);

	public abstract String getCreator();

}
