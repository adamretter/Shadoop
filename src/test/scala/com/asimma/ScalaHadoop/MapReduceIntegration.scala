package com.asimma.ScalaHadoop

import org.specs2.mutable._
import org.apache.hadoop.io.{LongWritable, Text}
import com.asimma.ScalaHadoop.MapReduceTaskChain._
import com.asimma.ScalaHadoop.typehelper.TextArrayWritable
import java.io.File
import scala.io.Source
import org.specs2.specification.{Step, Fragments}

class MapReduceIntegration extends Specification {

  "ScalaHadoop MapRecuce" should {

    "Pipe mapper output to reducer input" in {

      val outputPath = new File(new File(getClass().getResource("/").toURI), "mapReduceTest-output/pipe")
      outputPath.deleteOnExit() //cleanup after ourselves
      val inputPath = new File(getClass().getResource("/pipe-input.txt").toURI)

      val result = PipeTextMapReduceController.run(Array(inputPath.getAbsolutePath, outputPath.getAbsolutePath))

      val actualOutput = Source.fromFile(new File(outputPath, "/part-r-00000")).mkString

      val expectedOutputPath = new File(getClass().getResource("/pipe-output.txt").toURI).getAbsolutePath
      val expectedOutput = Source.fromFile(expectedOutputPath).mkString

      actualOutput mustEqual expectedOutput
    }

    "Chain types through default reducer and mapper" in {
      val outputPath = new File(new File(getClass().getResource("/").toURI), "mapReduceTest-output/chain")
      outputPath.deleteOnExit() //cleanup after ourselves
      val inputPath = new File(getClass().getResource("/chain-input.txt").toURI)

      val result = ChainTypesThroughDefaultsMapReduceController.run(Array(inputPath.getAbsolutePath, outputPath.getAbsolutePath))

      val actualOutput = Source.fromFile(new File(outputPath, "/part-r-00000")).mkString

      val expectedOutputPath = new File(getClass().getResource("/chain-output.txt").toURI).getAbsolutePath
      val expectedOutput = Source.fromFile(expectedOutputPath).mkString

      actualOutput mustEqual expectedOutput
    }
  }
}

/**
 * If we have the generic mapper and reducer:
 *  Mapper[K1, V1, K2, V2]
 *  Reducer[K2, V2, K3, V3]
 *
 * Rather than writing:
 *  MapReduceTask(mapper, reducer, "task name")
 *
 * we should be able to write:
 *  MapReduceTask(mapper, "mapper task") -->
 *    MapReduceTask(reducer, "reduce task)
 *
 * The default reducer in the first MapReduceTask
 * and default mapper in the second MapReduceTask must
 * correctly pass the Types of K and V through the chain
 */
object ChainTypesThroughDefaultsMapReduceController extends ScalaHadoop {

  val mapper = new Mapper[LongWritable, Text, Text, TextArrayWritable] {
    mapWith {
      (k, v) =>
        List(
          (new Text("TEST"), TextArrayWritable(List(v)))
        )
    }
  }

  val reducer = new Reducer[Text, TextArrayWritable, Text, Text] {
    reduceWith {
      (k, vs) =>
        for(v <- vs) yield (k, new Text(v.mkString(",")))
    }
  }

  def run(args: Array[String]): Int = {
    TextInput[Text, TextArrayWritable](args(0)) -->
      MapReduceTask(mapper, "ChainTypesThroughDefaultsMapTest") -->
        MapReduceTask(reducer, "ChainTypesThroughDefaultsReduceTest") -->
          TextOutput[Text, Text](args(1)) execute

    0
  }

}

object PipeTextMapReduceController extends ScalaHadoop {

  /**
   * Simply converts a line of text e.g. "a,b,c" from the input file into "k=a, v=[b,c]"
   */
  val mapper = new Mapper[LongWritable, Text, Text, TextArrayWritable] {
    mapWith {
      (k, v) =>
        val fields = v.toString.split(',')
        List((new Text(fields(0).trim), new TextArrayWritable(List(new Text(fields(1).trim), new Text(fields(2).trim)))))
    }
  }

  val reducer = new Reducer[Text, TextArrayWritable, Text, TextArrayWritable] {
    reduceWith {
      (k, vs) =>
        for(v <- vs) yield (k, v)
    }
  }

  def run(args: Array[String]): Int = {
    TextInput[Text, TextArrayWritable](args(0)) -->
      MapReduceTask(mapper, reducer, "PipeMapReduceTest") -->
      TextOutput[Text, TextArrayWritable](args(1)) execute

    0
  }
}
