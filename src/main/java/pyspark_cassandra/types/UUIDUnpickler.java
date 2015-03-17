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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.UUID;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;

public class UUIDUnpickler implements IObjectConstructor {
	@Override
	public Object construct(Object[] args) throws PickleException {
		if (args.length == 1) {
			return UUID.fromString((String) args[0]);
		} else {
			return new DictToUUIDConverter();
		}
	}

	public class DictToUUIDConverter {
		public UUID __setstate__(HashMap<String, Object> values) {
			BigInteger i = (BigInteger) values.get("int");
			ByteBuffer buffer = ByteBuffer.wrap(i.toByteArray());
			return new UUID(buffer.getLong(), buffer.getLong());
		}
	}
}