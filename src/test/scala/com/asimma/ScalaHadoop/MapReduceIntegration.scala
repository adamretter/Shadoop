package com.asimma.ScalaHadoop

import org.specs2.mutable._
import org.apache.hadoop.io.{LongWritable, Text}
import com.asimma.ScalaHadoop.MapReduceTaskChain._
import com.asimma.ScalaHadoop.typehelper.TextArrayWritable
import java.io.File
import scala.io.Source

class MapReduceIntegration extends Specification {

  "ScalaHadoop MapRecuce" should {
    "Correctly pipe mapper output to reducer input" in {

      val inputPath = new File(getClass().getResource("/integration-input.txt").toURI).getAbsolutePath
      val outputPath = new File(new File(getClass().getResource("/").toURI), "mapReduceTest-output").getAbsolutePath //TODO delete on cleanup, or here if exists!

      val result = MapReduceIntegrationController.run(Array(inputPath, outputPath))

      val actualOutput = Source.fromFile(outputPath + "/part-r-00000").mkString

      val expectedOutputPath = new File(getClass().getResource("/integration-output.txt").toURI).getAbsolutePath
      val expectedOutput = Source.fromFile(expectedOutputPath).mkString

      actualOutput mustEqual expectedOutput
    }
  }
}

object MapReduceIntegrationController extends ScalaHadoop {

  /**
   * Simply converts a line of text e.g. "a,b,c" from the input file into "k=a, v=[b,c]"
   */
  val mapper = new Mapper[LongWritable, Text, Text, TextArrayWritable] {
    mapWith {
      (k, v) => {
        val fields = v.toString.split(',')
        List((new Text(fields(0).trim), new TextArrayWritable(List(new Text(fields(1).trim), new Text(fields(2).trim)))))
      }
    }
  }

  val reducer = new Reducer[Text, TextArrayWritable, Text, TextArrayWritable] {
    reduceWith {
      (k, vs) => {
        for(v <- vs) yield (k, v)
      }
    }
  }

  def run(args: Array[String]): Int = {
    TextInput[Text, TextArrayWritable](args(0)) -->
      MapReduceTask(mapper, reducer, "MapReduceTest") -->
      TextOutput[Text, TextArrayWritable](args(1)) execute

    0
  }
}
