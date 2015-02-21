package pyspark_cassandra;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

public class AsStringPickler implements IObjectPickler {
	@Override
	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		pickler.save(o.toString());
	}
}
