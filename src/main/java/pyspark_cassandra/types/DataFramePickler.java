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
import java.nio.ByteBuffer;
import java.util.List;

import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.custom.IObjectPickler;
import net.razorvine.pickle.custom.Pickler;

public class DataFramePickler implements IObjectPickler {
	@SuppressWarnings("unchecked")
	public void pickle(Object o, OutputStream out, Pickler pickler) throws PickleException, IOException {
		DataFrame sdf = (DataFrame) o;
		
		List<?>[] columns = sdf.getColumnValues();
		Object[] columnArrays = new Object[columns.length];
		for (int i = 0; i < columnArrays.length; i++) {
			if (columns[i].get(0) instanceof ByteBuffer) {
				columnArrays[i] = new GatheredByteBuffers((List<ByteBuffer>) columns[i]);
			} else {
				columnArrays[i] = columns[i];
			}
		}

		out.write(Opcodes.GLOBAL);
		out.write("pyspark_cassandra.types\n_create_spanning_dataframe\n".getBytes());
		out.write(Opcodes.MARK);
		pickler.save(sdf.getColumnNames());
		pickler.save(sdf.getColumnTypes());
		pickler.save(columnArrays);
		out.write(Opcodes.TUPLE);
		out.write(Opcodes.REDUCE);
	}
}
