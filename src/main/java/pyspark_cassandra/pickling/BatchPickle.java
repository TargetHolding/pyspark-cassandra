package pyspark_cassandra.pickling;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Iterator;

import net.razorvine.pickle.custom.Pickler;

import org.apache.spark.api.java.function.FlatMapFunction;

import pyspark_cassandra.types.Types;

public class BatchPickle implements FlatMapFunction<Iterator<Object>, byte[]> {
	private static final long serialVersionUID = 1L;
	
	static {
		Types.registerCustomTypes();
	}

	private transient Pickler pickler = new Pickler();

	@Override
	public Iterable<byte[]> call(Iterator<Object> partition) throws Exception {
		return Arrays.asList(new byte[][] { this.pickler.dumps(partition) });
	}
	
	private void readObject(ObjectInputStream s) throws ClassNotFoundException, IOException {
		s.defaultReadObject();
		this.pickler = new Pickler();
	}
}
