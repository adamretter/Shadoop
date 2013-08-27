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
  }

}
