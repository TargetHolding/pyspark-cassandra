/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pyspark_cassandra

import pyspark_util.Conversions._
import pyspark_util.{ Pickling => PicklingUtils, _ }
import java.io.OutputStream
import java.math.BigInteger
import java.net.{ Inet4Address, Inet6Address, InetAddress }
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.{ ArrayList, Collection, HashMap, List => JList, Map => JMap, UUID }
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap.HashTrieMap
import scala.collection.immutable.List
import scala.collection.immutable.Map.{ Map1, Map2, Map3, Map4, WithDefault }
import scala.collection.mutable.{ ArraySeq, Buffer, WrappedArray }
import scala.reflect.runtime.universe.typeTag
import com.datastax.driver.core.{ ProtocolVersion, UDTValue => DriverUDTValue }
import com.datastax.spark.connector.UDTValue
import com.datastax.spark.connector.types.TypeConverter
import net.razorvine.pickle.{ IObjectConstructor, IObjectPickler, Opcodes, PickleUtils, Pickler, Unpickler }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import java.io.NotSerializableException
import com.datastax.spark.connector.GettableData

class Pickling extends PicklingUtils {
  override def register() {
    super.register()

    Unpickler.registerConstructor("pyspark.sql", "_create_row", PlainRowUnpickler)
    Unpickler.registerConstructor("pyspark_cassandra.types", "_create_row", PlainRowUnpickler)
    Unpickler.registerConstructor("pyspark_cassandra.types", "_create_udt", UDTValueUnpickler)

    Pickler.registerCustomPickler(classOf[Row], PlainRowPickler)
    Pickler.registerCustomPickler(classOf[UDTValue], UDTValuePickler)
    Pickler.registerCustomPickler(classOf[DriverUDTValue], DriverUDTValuePickler)
    Pickler.registerCustomPickler(classOf[DataFrame], DataFramePickler)

    TypeConverter.registerConverter(UnpickledUUIDConverter)
  }
}

object PlainRowPickler extends StructPickler {
  def creator = "pyspark_cassandra.types\n_create_row\n"
  def fields(o: Any) = o.asInstanceOf[Row].fields
  def values(o: Any, fields: Seq[_]) = o.asInstanceOf[Row].values
}

object PlainRowUnpickler extends StructUnpickler {
  def construct(fields: Seq[String], values: Seq[AnyRef]) = Row(fields, values)
}

object UDTValuePickler extends StructPickler {
  def creator = "pyspark_cassandra.types\n_create_udt\n"
  def fields(o: Any) = o.asInstanceOf[UDTValue].columnNames
  def values(o: Any, fields: Seq[_]) = o.asInstanceOf[UDTValue].columnValues
}

object DriverUDTValuePickler extends StructPickler {
  def creator = "pyspark_cassandra.types\n_create_udt\n"

  def fields(o: Any) = o.asInstanceOf[DriverUDTValue].getType().getFieldNames().toSeq

  def values(o: Any, fields: Seq[_]) = {
    val v = o.asInstanceOf[DriverUDTValue]
    v.getType().map {
      field => v.getObject(field.getName)
    }.toList
  }
}

object UDTValueUnpickler extends StructUnpickler {
  def construct(fields: Seq[String], values: Seq[AnyRef]) = {
    val f = asArray[String](fields)
    val v = asArray[AnyRef](values)
    UDTValue(f, v)
  }
}

object DataFramePickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    val df = o.asInstanceOf[DataFrame]

    val columns = df.values.map {
      v =>
        v(0) match {
          case c: ByteBuffer => new GatheredByteBuffers(v.asInstanceOf[List[ByteBuffer]])
          case c => c
        }
    }

    out.write(Opcodes.GLOBAL)
    out.write("pyspark_cassandra.types\n_create_spanning_dataframe\n".getBytes())
    out.write(Opcodes.MARK)
    pickler.save(df.names)
    pickler.save(df.types)
    pickler.save(columns)
    out.write(Opcodes.TUPLE)
    out.write(Opcodes.REDUCE)
  }
}

object UnpickledUUIDConverter extends TypeConverter[UUID] {
  val tt = typeTag[UUID]
  def targetTypeTag = tt
  def convertPF = { case holder: UUIDHolder => holder.uuid }
}
