package pyspark_cassandra;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

public class ByteBufferPickler implements IObjectPickler {
	@Override
	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		ByteBuffer buffer = (ByteBuffer) o;
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		pickler.save(bytes);
	}
}