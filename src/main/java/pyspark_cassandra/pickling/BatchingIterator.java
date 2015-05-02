package pyspark_cassandra.pickling;

import java.util.Iterator;

public class BatchingIterator<T> implements Iterator<Iterator<T>> {
	private Iterator<T> source;
	private int size;

	private int pos = 0;
	private BatchIterator batch = new BatchIterator();

	public BatchingIterator(Iterable<T> source, int size) {
		this(source.iterator(), size);
	}

	public BatchingIterator(Iterator<T> source, int size) {
		this.source = source;
		this.size = size;
	}

	@Override
	public boolean hasNext() {
		return source.hasNext();
	}

	@Override
	public Iterator<T> next() {
		pos = 0;
		return batch;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	private final class BatchIterator implements Iterator<T> {
		@Override
		public boolean hasNext() {
			return pos < size && source.hasNext();
		}

		@Override
		public T next() {
			pos += 1;
			return source.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}