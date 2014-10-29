/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.parsely.spark.converters

import java.lang.{Long => JLong}
import java.util.{GregorianCalendar, UUID, Date, Map => JMap, List => JList, Set => JSet}
import java.net.InetAddress
import java.nio.ByteBuffer
import collection.JavaConversions._

import com.google.gson.Gson
import org.apache.spark.Logging
import org.apache.spark.api.python.Converter
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.cassandra.serializers._
import com.datastax.driver.core.Row;


trait GenericCassandraSerializer {

  // TODO: deserialization, maybe look at https://github.com/apache/cassandra/blob/cassandra-2.1/src/java/org/apache/cassandra/hadoop/pig/AbstractCassandraStorage.java
  // for ideas
  // def deserializeFromCassandra(bb: ByteBuffer): Any = {

  // }

  def serializeToCassandra(obj: Any): ByteBuffer = obj match {
    case b:Boolean => BooleanSerializer.instance.serialize(b)
    case bd:BigDecimal => DecimalSerializer.instance.serialize(bd.underlying)
    case d:Double => DoubleSerializer.instance.serialize(d)
    case f:Float => FloatSerializer.instance.serialize(f)
    case iNetAddress:InetAddress => InetAddressSerializer.instance.serialize(iNetAddress)
    case i:Int => Int32Serializer.instance.serialize(i)
    case bi:BigInt => IntegerSerializer.instance.serialize(bi.underlying)
    // For lists, we only assume it contains [String] and is intended to
    // serialize to the CQL List collection
    case list:JList[_] =>
      val listSerializer = ListSerializer.getInstance(UTF8Serializer.instance)
      val serialized = listSerializer.serializeValues(list.asInstanceOf[JList[String]])
      CollectionSerializer.pack(serialized, list.size(), 1)
    case l:Long => LongSerializer.instance.serialize(l)

    // For maps, we only assume it contains [String, String] and is intended to
    // serialize to the CQL Map collection
    case map:JMap[_, _] =>
      val mapSerializer = MapSerializer.getInstance(UTF8Serializer.instance,
                                                    UTF8Serializer.instance)
      val serialized = mapSerializer.serializeValues(map.asInstanceOf[JMap[String, String]])
      CollectionSerializer.pack(serialized, map.size(), 1)

    // For sets, we only assume it contains [String] and is intended to
    // serialize to the CQL Set collection
    case set:JSet[_] =>
      val setSerializer = SetSerializer.getInstance(UTF8Serializer.instance)
      val serialized = setSerializer.serializeValues(set.asInstanceOf[JSet[String]])
      CollectionSerializer.pack(serialized, set.size(), 1)
    case cal:GregorianCalendar => TimestampSerializer.instance.serialize(cal.getTime())
    case date:Date => TimestampSerializer.instance.serialize(date)
    case str:String => UTF8Serializer.instance.serialize(str)
    case uuid:UUID =>
      uuid.version() match {
        case 1 => TimeUUIDSerializer.instance.serialize(uuid)
        case _ => UUIDSerializer.instance.serialize(uuid)
      }
    case x =>
      if (x == null) {
        EmptySerializer.instance.serialize(null)
      }
      else {
        throw new Exception(s"Cannot serialize: ${obj.getClass()}")
      }
  }
}


class FromUsersCQLKeyConverter extends Converter[Any, JLong] {
  override def convert(obj: Any): JLong = {
    obj.asInstanceOf[JLong]
  }
}


class FromUsersCQLValueConverter extends Converter[Any, JMap[String, Object]] {

  override def convert(obj: Any): JMap[String, Object] = {
    val row = obj.asInstanceOf[Row]
    // Probably shouldn't create this for every convert call, but Gson objects
    // are not serializable so we can't put this outside of the convert
    // definition
    val gson = new Gson()

    mapAsJavaMap(Map[String, Object](
      "user_id" -> row.getString("user_id"),
      "created_at" -> row.getDate("created_at"),
      "updated_at" -> row.getDate("updated_at"),
      "first_name" -> row.getString("first_name"),
      "last_name" -> row.getString("last_name"),
      // Another option here is probably custom Kyro serialization or something
      "emails" -> gson.toJson(row.getSet("emails", classOf[String])),
      "logins" -> gson.toJson(row.getList("logins", classOf[String])),
      "settings" -> gson.toJson(row.getMap("settings", classOf[String], classOf[String]))
    ))
  }
}


/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts
 * Java/Scala types to Cassandra types for partition and clustering keys.
 */
class ToCassandraCQLKeyConverter extends Converter[Any, java.util.Map[String, ByteBuffer]]
  with Logging with GenericCassandraSerializer {

  override def convert(obj: Any): java.util.Map[String, ByteBuffer] = {
      val input = obj.asInstanceOf[java.util.Map[String, Any]]
      val res = mapAsJavaMap(input.mapValues(i => serializeToCassandra(i)))
      return res
  }
}

class ToCassandraCQLValueConverter extends Converter[Any, java.util.List[ByteBuffer]]
  with Logging with GenericCassandraSerializer {

  override def convert(obj: Any): java.util.List[ByteBuffer] = {
    val input = obj.asInstanceOf[java.util.List[_]]
    seqAsJavaList(input.map(s => serializeToCassandra(s)))
  }
}
