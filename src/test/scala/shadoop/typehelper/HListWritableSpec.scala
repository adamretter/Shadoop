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
package shadoop.typehelper

import org.specs2.mutable._
import org.specs2.mutable.Specification
import org.apache.hadoop.io.{Text, LongWritable}


class HListWritableSpec extends Specification {

  "HListWritable" should {

    "be able to store and extract homogenous writable objects" in {

      val hlw = new Text("text1") :: new Text("text2") :: HListWritableNil

      hlw.length must beEqualTo(2)
    }

    "be able to store heterogenous writable objects" in {

      val hlw = new Text("text1") :: new LongWritable(123) :: HListWritableNil

      hlw.length must beEqualTo(2)
    }


    "be able to restore heterogenous writable objects" in {

      val item1 = new Text("text1")
      val item2 = new LongWritable(123)

      val hlw = item1 :: item2 :: HListWritableNil

      val resultItem1: Text = hlw.head
      val resultItem2: LongWritable = hlw.tail.head

      item1.equals(resultItem1) must beEqualTo(true)
      item2.equals(resultItem2) must beEqualTo(true)
    }
  }

}
