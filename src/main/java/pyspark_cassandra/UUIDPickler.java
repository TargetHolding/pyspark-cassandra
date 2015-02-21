package pyspark_cassandra;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

public class UUIDPickler implements IObjectPickler {
	@Override
	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		UUID uuid = (UUID) o;

		out.write(Opcodes.GLOBAL);
		out.write("uuid\nUUID\n".getBytes());
		out.write(Opcodes.MARK);
		pickler.save(uuid.toString());
		out.write(Opcodes.TUPLE);
		out.write(Opcodes.REDUCE);
	}
}