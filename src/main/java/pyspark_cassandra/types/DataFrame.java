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

import java.util.List;

public class DataFrame {
	private String[] names;
	private String[] types;
	private List<?>[] values;

	public DataFrame(String[] names, String[] types, List<?>[] values) {
		this.names = names;
		this.types = types;
		this.values = values;
	}

	public String[] getColumnNames() {
		return names;
	}

	public String[] getColumnTypes() {
		return types;
	}

	public List<?>[] getColumnValues() {
		return values;
	}
}
