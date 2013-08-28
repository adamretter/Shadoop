/**
 * Copyright 2013
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asimma.ScalaHadoop

import org.apache.hadoop.io.serializer.{Deserializer, WritableSerialization}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.conf.{Configuration, Configured}
import java.io.{IOException, DataInputStream, InputStream}
import org.apache.hadoop.util.ReflectionUtils

/**
 * A version of Hadoop WritableSerialization which does not re-use objects
 */
class ImmutableWritableSerialization extends WritableSerialization {

  override def getDeserializer(writableClass: Class[Writable]) = new ImmutableWritableDeserializer(getConf(), writableClass);
}

/**
 * Hadoop's org.apache.hadoop.io.serializer.WritableSerialization.WritableDeserializer
 * can re-use instances of Writable objects by modifying the
 * reference of their content.
 * See the Hint on page 236 of the O'Reilly book "Hadoop, The Definitive Guide (3rd-edition)"
 * and study the code of the Hadoop class, particularly the deserialize(org.apache.hadoop.io.Writable) method
 *
 * The Hadoop approach is Mutable
 * which is somewhat at odd's with what we are doing here in Scala
 * and can sometimes lead to head scratching issues!
 *
 * This class implements an Immutable version of the deserializer which
 * never re-uses objects, but rather creates new objects
 * each time.
 */
class ImmutableWritableDeserializer(conf: Configuration, writableClass: Class[Writable]) extends Configured(conf) with Deserializer[Writable] {
  private var dataIn : DataInputStream = null;

  def open(in: InputStream) {
    dataIn = in match {
      case dis: DataInputStream =>
            dis
      case _ =>
        new DataInputStream(in)

    }
  }

  @throws(classOf[IOException])
  def deserialize(w: Writable) = {
    val writable = ReflectionUtils.newInstance(writableClass, getConf())
    writable.readFields(dataIn)
    writable
  }

  @throws(classOf[IOException])
  def close() {
    dataIn.close()
  }
}
