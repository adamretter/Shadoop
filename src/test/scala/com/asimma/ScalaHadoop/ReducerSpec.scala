/**
 * Copyright (C) 2013 Adam Retter (adam.retter@googlemail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asimma.ScalaHadoop

import org.specs2.mutable.Specification
import org.apache.hadoop.io.Text
import com.asimma.ScalaHadoop.typehelper.TextArrayWritable

class ReducerSpec extends Specification {

  val reducer = new Reducer[Text, TextArrayWritable, Text, TextArrayWritable] {
    reduceWith {
      (k, vs) =>
        for(v <- vs) yield (k, v)
    }
  }

  "reducer" should {
    "identity transform when given distinct TextArrayWritables" in {

      import scala.collection.JavaConverters._

      val testKey = new Text("TEST")
      val testValues = (1 to 5).map(i => TextArrayWritable((1 to 3).map(j => new Text(s"text_$i-$j"))))

      reducer.testF(testKey, testValues.asJava) mustEqual Some(testValues.map((testKey, _)))
    }


    "identity transform given references of singular TextArrayWritables e.g. Hadoop object re-use" in {

      import scala.collection.JavaConverters._

      val testKey = new Text("TEST")

        /**
         * Simulate mutable object re-use as done by Hadoop
         * in it's org.apache.hadoop.io.serializer.WritableSerialization.WritableDeserializer#deserialize(org.apache.hadoop.io.Writable)
         * see the hint on page 236 of the O'Reily book "Hadoop, The Definitive Guide (3rd-edition)"
         */
      var array: Option[TextArrayWritable] = None
      val testValues = (1 to 5).map(
        i =>
          array match {
            case Some(a) =>
              a.set(
                (1 to 3).map(j => new Text(s"text_$i-$j")).toArray
              )
              a
            case None =>
              array = Some(new TextArrayWritable(
                (1 to 3).map(j => new Text(s"text_$i-$j"))
              ))
              array.get
          }
      )

      reducer.testF(testKey, testValues.asJava) mustEqual Some(testValues.map((testKey, _)))
    }
  }

}
