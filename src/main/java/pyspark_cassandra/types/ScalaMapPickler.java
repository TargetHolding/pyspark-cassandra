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

import java.io.IOException;
import java.io.OutputStream;

import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.custom.IObjectPickler;
import net.razorvine.pickle.custom.Pickler;
import scala.collection.Iterator;
import scala.collection.Map;

public class ScalaMapPickler implements IObjectPickler {
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		Map map = (Map) o;

		out.write(Opcodes.EMPTY_DICT);
		pickler.writeMemo(o);
		out.write(Opcodes.MARK);

		Iterator keys = map.keysIterator();
		Object k;
		
		while (keys.hasNext()) {
			k = keys.next();
			pickler.save(k);
			pickler.save(map.apply(k));
		}

		out.write(Opcodes.SETITEMS);
	}
}
