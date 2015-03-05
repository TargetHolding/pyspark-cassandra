/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pyspark_cassandra.types;

import java.util.ArrayList;
import java.util.Arrays;
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
		return toArraySeq(Arrays.asList(elements));
	}

	public static <E> ArraySeq<E> toArraySeq(List<E> elements) {
		ArraySeq<E> seq = new ArraySeq<E>(elements.size());
		
		for (int i = 0; i < elements.size(); i++) {
			seq.update(i, elements.get(i));
		}
		
		return seq;
	}
}
