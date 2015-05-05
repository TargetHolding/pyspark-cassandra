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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import net.razorvine.pickle.custom.Pickler;
import net.razorvine.pickle.custom.Unpickler;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.UDTValue;

public class Types {
	static {
		registerCustomTypes();
	}

	public static void registerCustomTypes() {
		Unpickler.registerConstructor("uuid", "UUID", new UUIDUnpickler());
		Unpickler.registerConstructor("pyspark.sql", "_create_row", new CassandraRowUnpickler());
		Unpickler.registerConstructor("pyspark_cassandra.types", "_create_row", new CassandraRowUnpickler());
		Unpickler.registerConstructor("pyspark_cassandra.types", "_create_udt", new UDTValueUnpickler());

		Pickler.registerCustomPickler(UUID.class, new UUIDPickler());
		Pickler.registerCustomPickler(InetAddress.class, new AsStringPickler());
		Pickler.registerCustomPickler(Inet4Address.class, new AsStringPickler());
		Pickler.registerCustomPickler(Inet6Address.class, new AsStringPickler());
		Pickler.registerCustomPickler(ByteBuffer.class, new ByteBufferPickler());
		Pickler.registerCustomPickler(CassandraRow.class, new CassandraRowPickler());
		Pickler.registerCustomPickler(UDTValue.class, new UDTValuePickler());
	}

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

	public static <E> ArraySeq<E> toArraySeq(List<E> elements) {
		ArraySeq<E> seq = new ArraySeq<E>(elements.size());

		for (int i = 0; i < elements.size(); i++) {
			seq.update(i, elements.get(i));
		}

		return seq;
	}
}
