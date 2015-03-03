package pyspark_cassandra.types;

import java.util.ArrayList;
import java.util.List;

import scala.collection.Seq;

public class Types {
	public static <T> List<T> toList(T[] arr) {
		List<T> list = new ArrayList<T>(arr.length);

		for (T v : arr) {
			list.add(v);
		}

		return list;
	}

	public static <T> List<T> toList(Seq<T> seq) {
		List<T> list = new ArrayList<T>(seq.size());

		for (int i = 0; i < seq.size(); i++) {
			list.add(seq.apply(i));
		}

		return list;
	}
}
