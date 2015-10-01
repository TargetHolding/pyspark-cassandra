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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UserType.Field;

public class UDTValuePickler extends StructPickler {
	// TODO this is a plain hack ... the protocol version might just as well _not_ be the newest
	private static final ProtocolVersion PROTOCOL_VERSION = ProtocolVersion.NEWEST_SUPPORTED;

	public String getCreator() {
		return "pyspark_cassandra.types\n_create_udt\n";
	}

	@Override
	public Collection<String> getFieldNames(Object o) {
		//The returned generic Collection does not guarantee order and it would sometimes turn into 
		//a python set -- we need the fieldnames to be sorted for pickling!
		return new ArrayList(((UDTValue) o).getType().getFieldNames());
	}

	@Override
	public Collection<Object> getFieldValues(Object o) {
		UDTValue udtValue = (UDTValue) o;
		UserType type = udtValue.getType();

		Collection<Object> values = new ArrayList<Object>(type.size());

		for (Field field : type) {
			ByteBuffer bytes = udtValue.getBytesUnsafe(field.getName());
			Object value = field.getType().deserialize(bytes, PROTOCOL_VERSION);
			values.add(value);
		}

		return values;
	}
}
