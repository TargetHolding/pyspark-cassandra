package pyspark_cassandra.pickling;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.custom.Pickler;

import org.apache.spark.api.java.function.FlatMapFunction;

import pyspark_cassandra.types.Types;

public class BatchPickle implements FlatMapFunction<Iterator<?>, byte[]> {
	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_BATCH_SIZE = 1000;

	static {
		Types.registerCustomTypes();
	}

	private transient Pickler pickler = new Pickler();
	private int batchSize;

	public BatchPickle() {
		this(DEFAULT_BATCH_SIZE);
	}

	public BatchPickle(int batchSize) {
		this.batchSize = batchSize;
	}

	@Override
	public Iterable<byte[]> call(Iterator<?> partition) throws Exception {
		final BatchingIterator<?> bi = new BatchingIterator<>(partition, this.batchSize);

		return new Iterable<byte[]>() {
			@Override
			public Iterator<byte[]> iterator() {
				return new Iterator<byte[]>() {
					@Override
					public boolean hasNext() {
						return bi.hasNext();
					}

					@Override
					public byte[] next() {
						try {
							return pickler.dumps(bi.next());
						} catch (PickleException | IOException e) {
							throw new RuntimeException(e);
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	private void readObject(ObjectInputStream s) throws ClassNotFoundException, IOException {
		s.defaultReadObject();
		this.pickler = new Pickler();
	}
}
