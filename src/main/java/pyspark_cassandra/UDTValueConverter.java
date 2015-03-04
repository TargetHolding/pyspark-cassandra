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

package pyspark_cassandra;

import pyspark_cassandra.types.Types;
import scala.collection.IndexedSeq;

import com.datastax.spark.connector.UDTValue;

public class UDTValueConverter {
	private String[] fieldNames;
	private Object[] fieldValues;

	public UDTValueConverter(String[] fieldNames, Object[] fieldValues) {
		super();
		this.fieldNames = fieldNames;
		this.fieldValues = fieldValues;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public Object[] getFieldValues() {
		return fieldValues;
	}

	public UDTValue toConnectorType() {
		IndexedSeq<String> fieldNamesSeq = Types.toArraySeq(this.fieldNames);
		IndexedSeq<Object> fieldValuesSeq = Types.toArraySeq(this.fieldValues);
		return new UDTValue(fieldNamesSeq, fieldValuesSeq);
	}
}
