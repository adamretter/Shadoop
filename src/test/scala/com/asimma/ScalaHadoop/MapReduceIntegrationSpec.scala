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

import org.specs2.mutable._
import org.apache.hadoop.io.{LongWritable, Text}
import com.asimma.ScalaHadoop.MapReduceTaskChain._
import com.asimma.ScalaHadoop.typehelper.TextArrayWritable
import java.io.File
import scala.io.Source
import org.specs2.specification.{Step, Fragments}
import org.apache.hadoop.conf.Configuration

class MapReduceIntegrationSpec extends Specification {

  sequential


  "ScalaHadoop MapReduce" should {

    "Pipe mapper output to mutable reducer input" in {
      testController(
        controller = PipeMapMutableReduceController,
        inputTextFile = "pipe-input.txt",
        expectedOutputTextFile = "pipe-output-mutable.txt"
      )
    }

    "Pipe mapper output to immutable reducer input" in {
      testController(
        controller = PipeMapImmutableReduceController,
        inputTextFile = "pipe-input.txt",
        expectedOutputTextFile = "pipe-output-immutable.txt"
      )
    }

    "Chain different types reducer and mapper" in {
      testController(
        controller = ChainDifferentTypesMapReduceController,
        inputTextFile = "chain-input.txt",
        expectedOutputTextFile = "chain-output.txt"
      )
    }

    "Chain different types through default reducer and mapper" in {
      testController(
        controller = ChainDifferentTypesThroughDefaultsMapReduceController,
        inputTextFile = "chain-input.txt",
        expectedOutputTextFile = "chain-output.txt"
      )
    }
  }

  def testController(inputTextFile: String, expectedOutputTextFile: String, controller: ScalaHadoop) = {
    val outputPath = new File(new File(getClass().getResource("/").toURI), s"mapReduceTest-output/${controller.getClass.getName}")
    outputPath.deleteOnExit() //cleanup after ourselves
    val inputPath = new File(getClass().getResource(s"/$inputTextFile").toURI)

    val result = controller.run(Array(inputPath.getAbsolutePath, outputPath.getAbsolutePath))

    val actualOutput = Source.fromFile(new File(outputPath, "/part-r-00000")).mkString

    val expectedOutputPath = new File(getClass().getResource(s"/$expectedOutputTextFile").toURI).getAbsolutePath
    val expectedOutput = Source.fromFile(expectedOutputPath).mkString

    actualOutput mustEqual expectedOutput
  }
}

class ToArrayMapper extends Mapper[LongWritable, Text, Text, TextArrayWritable] {
  mapWith {
    (k, v) =>
      List(
        (new Text("TEST"), TextArrayWritable(List(v)))
      )
  }
}

class FromArrayReducer extends Reducer[Text, TextArrayWritable, Text, Text] {
  reduceWith {
    (k, vs) =>
      for(v <- vs) yield (k, new Text(v.mkString(",")))
  }
}

/**
 * If we have the generic mapper and reducer:
 *  Mapper[K1, V1, K2, V2]
 *  Reducer[K2, V2, K3, V3]
 *
 * We can write:
 *  MapReduceTask(mapper, reducer, "task name")
 */
object ChainDifferentTypesMapReduceController extends ScalaHadoop {
  def run(args: Array[String]): Int = {
    TextInput[Text, TextArrayWritable](args(0)) -->
      MapReduceTask(new ToArrayMapper(), new FromArrayReducer(), "ChainDifferentTypesMapReduceTest") -->
        TextOutput[Text, Text](args(1)) execute

    0
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
 * correctly pass the Types of Kn and Vn through the chain
 */
object ChainDifferentTypesThroughDefaultsMapReduceController extends ScalaHadoop {

  def run(args: Array[String]): Int = {
    TextInput[Text, TextArrayWritable](args(0)) -->
      MapReduceTask(new ToArrayMapper(), "ChainTypesThroughDefaultsMapTest") -->
        MapReduceTask(new FromArrayReducer(), "ChainTypesThroughDefaultsReduceTest") -->
          TextOutput[Text, Text](args(1)) execute

    0
  }

}

/**
 * Simply converts a line of text e.g. "a,b,c,...n" from the input file into "k=a, v=[b,c,...n]"
 */
class FirstValueIsKeyMapper extends Mapper[LongWritable, Text, Text, TextArrayWritable] {
  mapWith {
    (k, v) =>
      val fields = v.toString.split(',')
      List((new Text(fields.head.trim), new TextArrayWritable(fields.tail.map(s => new Text(s.trim)))))
  }
}

class IdentityReducer extends com.asimma.ScalaHadoop.identity.IdentityReducer[Text, TextArrayWritable]

object PipeMapMutableReduceController extends ScalaHadoop {
  def run(args: Array[String]): Int = {
    TextInput[Text, TextArrayWritable](args(0)) -->
      MapReduceTask(new FirstValueIsKeyMapper(), new IdentityReducer(), "PipeMapMutableReduceTest") -->
      TextOutput[Text, TextArrayWritable](args(1)) execute

    0
  }
}

object PipeMapImmutableReduceController extends ScalaHadoop {

  val immutableDeserializationConfModifier = new ConfModifier {
    def apply(c: Configuration) {
      c.set("io.serializations", classOf[ImmutableWritableSerialization].getName)
    }
  }

  def run(args: Array[String]): Int = {
    TextInput[Text, TextArrayWritable](args(0)) -->
      MapReduceTask(new FirstValueIsKeyMapper(), new IdentityReducer(), "PipeMapImmutableReduceTest") -->
        immutableDeserializationConfModifier -->
        TextOutput[Text, TextArrayWritable](args(1)) execute

    0
  }
}
