package pyspark_cassandra;

import java.util.Iterator;

import com.google.common.collect.Iterators;

public class EmptyIterable<T> implements Iterable<T> {
	@SuppressWarnings("rawtypes")
	private static EmptyIterable instance = new EmptyIterable();

	@Override
	public Iterator<T> iterator() {
		return Iterators.emptyIterator();
	}

	@SuppressWarnings("unchecked")
	public static <T> EmptyIterable<T> get() {
		return instance;
	}
}