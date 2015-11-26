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

import java.io.OutputStream
import java.math.BigInteger
import java.net.{ Inet4Address, Inet6Address, InetAddress }
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.{
  ArrayList,
  Collection,
  HashMap,
  List => JList,
  Map => JMap,
  UUID
}

import scala.reflect.ClassTag
import scala.collection.JavaConversions.{
  asScalaBuffer,
  bufferAsJavaList,
  collectionAsScalaIterable,
  iterableAsScalaIterable,
  mapAsJavaMap,
  seqAsJavaList
}
import scala.collection.immutable.HashMap.HashTrieMap
import scala.collection.immutable.List
import scala.collection.immutable.Map.{ Map1, Map2, Map3, Map4, WithDefault }
import scala.collection.mutable.{Buffer,WrappedArray}
import scala.reflect.runtime.universe.typeTag

import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.{ UDTValue => DriverUDTValue }
import com.datastax.spark.connector.UDTValue
import com.datastax.spark.connector.types.TypeConverter

import net.razorvine.pickle.IObjectConstructor
import net.razorvine.pickle.IObjectPickler
import net.razorvine.pickle.Opcodes
import net.razorvine.pickle.PickleUtils
import net.razorvine.pickle.Pickler
import net.razorvine.pickle.Unpickler

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object Pickling {
  def register() = {
    Unpickler.registerConstructor("uuid", "UUID", UUIDUnpickler)
    Unpickler.registerConstructor("pyspark.sql", "_create_row", PlainRowUnpickler)
    Unpickler.registerConstructor("pyspark_cassandra.types", "_create_row", PlainRowUnpickler)
    Unpickler.registerConstructor("pyspark_cassandra.types", "_create_udt", UDTValueUnpickler)

    Pickler.registerCustomPickler(classOf[UUID], UUIDPickler)
    Pickler.registerCustomPickler(classOf[InetAddress], AsStringPickler)
    Pickler.registerCustomPickler(classOf[Inet4Address], AsStringPickler)
    Pickler.registerCustomPickler(classOf[Inet6Address], AsStringPickler)
    Pickler.registerCustomPickler(classOf[ByteBuffer], ByteBufferPickler)
    Pickler.registerCustomPickler(Class.forName("java.nio.HeapByteBuffer"), ByteBufferPickler)
    Pickler.registerCustomPickler(classOf[GatheredByteBuffers], GatheringByteBufferPickler)
    Pickler.registerCustomPickler(classOf[Row], PlainRowPickler)
    Pickler.registerCustomPickler(classOf[DriverUDTValue], UDTValuePickler)
    Pickler.registerCustomPickler(classOf[DataFrame], DataFramePickler)
    Pickler.registerCustomPickler(Class.forName("scala.collection.immutable.$colon$colon"), ListPickler)
    Pickler.registerCustomPickler(classOf[WrappedArray.ofRef[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Stream.Cons[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple1[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple2[_, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple3[_, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple4[_, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple5[_, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple6[_, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple7[_, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple8[_, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple9[_, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple10[_, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple11[_, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple12[_, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple13[_, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple14[_, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[WithDefault[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map1[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map2[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map3[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map4[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[HashTrieMap[_, _]], MapPickler)

    TypeConverter.registerConverter(UUIDUnpickler.UnpickledUUIDConverter)
  }
}

class PicklableRDD(rdd: RDD[_]) {
  def pickle() = rdd.mapPartitions(new BatchPickler(), true)
}

class UnpicklableRDD(rdd: RDD[Array[Byte]]) {
  def unpickle() = rdd.flatMap(new BatchUnpickler())
}

class UnpicklableDStream(dstream: DStream[Array[Byte]]) {
  def unpickle() = dstream.flatMap(new BatchUnpickler())
}

class BatchPickler(batchSize: Int = 1000)
    extends (Iterator[_] => Iterator[Array[Byte]])
    with Serializable {

  def apply(in: Iterator[_]): Iterator[Array[Byte]] = {
    val pickler = new Pickler()
    in.grouped(batchSize).map { b => pickler.dumps(b.toArray) }
  }
}

class BatchUnpickler extends (Array[Byte] => Seq[Any]) with Serializable {
  def apply(in: Array[Byte]): Seq[Any] = {
    val unpickled = new Unpickler().loads(in)
    Utils.asSeq(unpickled)
  }
}

trait StructPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL)
    out.write(creator.getBytes())
    out.write(Opcodes.MARK)
    val f = fields(o)
    pickler.save(f)
    pickler.save(values(o, f))
    out.write(Opcodes.TUPLE)
    out.write(Opcodes.REDUCE)
  }

  def creator: String
  def fields(o: Any): Seq[_]
  def values(o: Any, fields: Seq[_]): Seq[_]
}

trait StructUnpickler extends IObjectConstructor {
  def construct(args: Array[AnyRef]): Object = {
    val fields = Utils.asArray[String](args(0))
    val values = Utils.asArray[AnyRef](args(1))

    construct(fields, values)
  }

  def construct(fields: Array[String], values: Array[AnyRef]): Object
}

object PlainRowPickler extends StructPickler {
  def creator = "pyspark_cassandra.types\n_create_row\n"
  def fields(o: Any) = o.asInstanceOf[Row].fields
  def values(o: Any, fields: Seq[_]) = o.asInstanceOf[Row].values
}

object PlainRowUnpickler extends StructUnpickler {
  def construct(fields: Array[String], values: Array[AnyRef]) = Row(fields, values)
}

object UDTValuePickler extends StructPickler {
  def creator = "pyspark_cassandra.types\n_create_udt\n"

  def fields(o: Any) = o.asInstanceOf[DriverUDTValue].getType().getFieldNames().toSeq

  def values(o: Any, fields: Seq[_]) = {
    // TODO this is a plain hack ... the protocol version might just as well _not_ be the newest
    val pv = ProtocolVersion.NEWEST_SUPPORTED

    val v = o.asInstanceOf[DriverUDTValue]
    v.getType().map {
      field =>
        if (v.isNull(field.getName))
          null
        else
          field.getType().deserialize(v.getBytesUnsafe(field.getName), pv)
    }.toList
  }
}

object UDTValueUnpickler extends StructUnpickler {
  def construct(fields: Array[String], values: Array[AnyRef]) = UDTValue(fields, values)
}

object AsStringPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler) = pickler.save(o.toString())
}

object UUIDPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL)
    out.write("uuid\nUUID\n".getBytes())
    out.write(Opcodes.MARK)
    pickler.save(o.asInstanceOf[UUID].toString())
    out.write(Opcodes.TUPLE)
    out.write(Opcodes.REDUCE)
  }
}

object UUIDUnpickler extends IObjectConstructor {
  def construct(args: Array[Object]): Object = {
    args.size match {
      case 1 => UUID.fromString(args(0).asInstanceOf[String])
      case _ => new UUIDHolder()
    }
  }

  class UUIDHolder {
    var uuid: Option[UUID] = None

    def __setstate__(values: HashMap[String, Object]): UUID = {
      val i = values.get("int").asInstanceOf[BigInteger]
      val buffer = ByteBuffer.wrap(i.toByteArray())
      uuid = Some(new UUID(buffer.getLong(), buffer.getLong()))
      return uuid.get
    }
  }

  object UnpickledUUIDConverter extends TypeConverter[UUID] {
    def targetTypeTag = typeTag[UUID]
    def convertPF = { case holder: UUIDHolder => holder.uuid.get }
  }
}

class GatheredByteBuffers(buffers: Seq[ByteBuffer]) extends Iterable[ByteBuffer] {
  def iterator() = buffers.iterator
}

class GatheringByteBufferPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, currentPickler: Pickler) = {
    val buffers = o.asInstanceOf[GatheredByteBuffers]

    out.write(Opcodes.GLOBAL)
    out.write("__builtin__\nbytearray\n".getBytes())

    out.write(Opcodes.BINSTRING)

    val length = buffers.map { _.remaining() }.sum

    out.write(PickleUtils.integer_to_bytes(length))

    val c = Channels.newChannel(out)
    buffers.foreach { c.write(_) }

    out.write(Opcodes.TUPLE1)
    out.write(Opcodes.REDUCE)
  }
}

object GatheringByteBufferPickler extends GatheringByteBufferPickler

object ByteBufferPickler extends GatheringByteBufferPickler {
  override def pickle(o: Any, out: OutputStream, pickler: Pickler) = {
    val buffers = new GatheredByteBuffers(o.asInstanceOf[ByteBuffer] :: Nil)
    super.pickle(buffers, out, pickler)
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

object ListPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    pickler.save(
      o match {
        case c: Collection[_] => c
        case b: Buffer[_] => bufferAsJavaList(b)
        case s: Seq[_] => seqAsJavaList(s)
        case p: Product => seqAsJavaList(p.productIterator.toSeq)
      })
  }
}

object MapPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    pickler.save(
      o match {
        case m: JMap[_, _] => m
        case m: Map[_, _] => mapAsJavaMap(m)
      })
  }
}
