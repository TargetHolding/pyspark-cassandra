package pyspark_cassandra.pickling;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import net.razorvine.pickle.custom.Unpickler;

import org.apache.spark.api.java.function.FlatMapFunction;

import pyspark_cassandra.types.Types;

public class BatchUnpickle implements FlatMapFunction<byte[], Object> {
	private static final long serialVersionUID = 1L;

	static {
		Types.registerCustomTypes();
	}

	private transient Unpickler unpickler = new Unpickler();

	@Override
	@SuppressWarnings("unchecked")
	public Iterable<Object> call(byte[] pickledata) throws Exception {
		return (List<Object>) this.unpickler.loads(pickledata);
	}

	private void readObject(ObjectInputStream s) throws ClassNotFoundException, IOException {
		s.defaultReadObject();
		this.unpickler = new Unpickler();
	}
}
