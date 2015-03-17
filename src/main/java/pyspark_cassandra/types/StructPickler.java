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
import java.util.List;

import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.custom.IObjectPickler;
import net.razorvine.pickle.custom.Pickler;
import scala.collection.IndexedSeq;

public abstract class StructPickler implements IObjectPickler {

	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		List<String> fieldNames = Types.toJavaList(this.getFieldNames(o));
		List<Object> fieldValues = Types.toJavaList(this.getFieldValues(o));

		out.write(Opcodes.GLOBAL);
		out.write(this.getCreator().getBytes());
		out.write(Opcodes.MARK);
		pickler.save(fieldNames);
		pickler.save(fieldValues);
		out.write(Opcodes.TUPLE);
		out.write(Opcodes.REDUCE);
	}

	public abstract IndexedSeq<String> getFieldNames(Object o);

	public abstract IndexedSeq<Object> getFieldValues(Object o);

	public abstract String getCreator();

}
