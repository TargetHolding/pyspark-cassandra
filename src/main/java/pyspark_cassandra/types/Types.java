package pyspark_cassandra.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

public class Types {
	public static <T> List<T> toJavaList(T[] arr) {
		List<T> list = new ArrayList<T>(arr.length);

		for (T v : arr) {
			list.add(v);
		}

		return list;
	}

	public static <T> List<T> toJavaList(Seq<T> seq) {
		List<T> list = new ArrayList<T>(seq.size());

		for (int i = 0; i < seq.size(); i++) {
			list.add(seq.apply(i));
		}

		return list;
	}

	public static <K, V> java.util.Map<K, V> toJavaMap(Map<K, V> map) {
		HashMap<K, V> hashmap = new HashMap<K, V>(map.size());

		for (Iterator<Tuple2<K, V>> it = map.iterator(); it.hasNext();) {
			Tuple2<K, V> kv = it.next();
			hashmap.put(kv._1, kv._2);
		}

		return hashmap;
	}

	public static <E> ArraySeq<E> toArraySeq(E[] elements) {
		ArraySeq<E> seq = new ArraySeq<E>(elements.length);
		
		for (int i = 0; i < elements.length; i++) {
			seq.update(i, elements[i]);
		}

		return seq;
	}
}
