package pyspark_cassandra.types;

import java.io.IOException;
import java.io.OutputStream;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import scala.collection.Map;

public class ScalaMapPickler implements IObjectPickler {
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		pickler.save(Types.toJavaMap((Map) o));
	}
}
