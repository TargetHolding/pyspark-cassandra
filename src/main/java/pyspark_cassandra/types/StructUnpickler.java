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
import java.util.List;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;

public abstract class StructUnpickler implements IObjectConstructor {
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object construct(Object[] args) throws PickleException {
		List<String> fieldNames = (ArrayList<String>) args[0];
		List<Object> fieldValues = args[1] instanceof List ? (List) args[1] : Types.toJavaList((Object[]) args[1]);

		return this.construct(fieldNames, fieldValues);
	}

	public abstract Object construct(List<String> fieldNames, List<Object> fieldValues);
}
