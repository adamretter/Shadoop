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
