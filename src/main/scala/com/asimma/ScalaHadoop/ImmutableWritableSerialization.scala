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
import scala.reflect.runtime.universe.TypeTag


class ImmutableWritableSerialization extends WritableSerialization {

  override def getDeserializer(writableClass: Class[Writable]) = new ImmutableWritableDeserializer(getConf(), writableClass);
}

class ImmutableWritableDeserializer[T <: Class[_ <: Writable]](conf: Configuration, writableClass: T)(implicit ttWritableClass: TypeTag[T]) extends Configured(conf) with Deserializer[Writable] {
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
    def newInstance() = {
      import scala.reflect.runtime.{universe => ru}
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val typ = ru.typeOf(ttWritableClass)
      val c = typ.typeSymbol.asClass
      val cm = m.reflectClass(c)
      val ctor = typ.declaration(ru.nme.CONSTRUCTOR).asMethod
      val ctorm = cm.reflectConstructor(ctor)

      ctorm(getConf()).asInstanceOf[Writable]
    }

    val writable = newInstance()
    writable.readFields(dataIn)
    writable
  }

  @throws(classOf[IOException])
  def close() {
    dataIn.close()
  }
}
