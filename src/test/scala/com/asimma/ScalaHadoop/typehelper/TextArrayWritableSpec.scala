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
package com.asimma.ScalaHadoop.typehelper

import org.specs2.mutable._
import org.apache.hadoop.io.Text

class TextArrayWritableSpec extends Specification {


  "TextArrayWritable" should {

      "read same data as was written" in {

        val data = for(i <- 1 to 10) yield new Text(s"Thing $i")

        val array = new TextArrayWritable(data)

        array must containTheSameElementsAs(data)
      }

    "apply as object" in {
      val data = for(i <- 1 to 10) yield new Text(s"Thing $i")

      val array = TextArrayWritable(data.toSeq)

      array.getClass must beEqualTo(classOf[TextArrayWritable])
    }
  }

  ":+" should {
    "append a value" in {

      val data = List(new Text("item1"), new Text("item2"))
      val arrayWritable = new TextArrayWritable(data)

      val extra = new Text("item3")
      val result = arrayWritable :+ extra

      result.toList mustEqual (data :+ extra)
    }
  }

  "+:" should {
    "prepend a value" in {

      val data = List(new Text("item1"), new Text("item2"))
      val arrayWritable = new TextArrayWritable(data)

      val extra = new Text("item3")
      val result = extra +: arrayWritable

      result.toList mustEqual (extra +: data)
    }
  }

}
