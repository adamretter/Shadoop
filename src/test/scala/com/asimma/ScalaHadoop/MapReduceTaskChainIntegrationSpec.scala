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

import org.specs2.mutable.Specification
import org.apache.hadoop.conf.Configuration
import com.asimma.ScalaHadoop.MapReduceTaskChain._
import com.asimma.ScalaHadoop.MapReduceTaskChain.ConfModifier
import java.io.File
import org.apache.hadoop.io.{Text, LongWritable}
import scala.io.Source
import com.asimma.ScalaHadoop.identity.IdentityMapper

class MapReduceTaskChainIntegrationSpec extends Specification {

  "confModifier --> " should {

    "be overrideable by second confModifier -->" in {
      val testProp = ("some-test-key", "some-test-value")
      val overridenValue = "overriden-test-value"
      val overridenValue2 = "overriden-test-value2"

      val conf = new Configuration()
      conf.set(testProp._1, testProp._2)

      val confModifier = new ConfModifier {
        def apply(c: Configuration) {
          c.set(testProp._1, overridenValue)
        }
      }

      val confModifier2 = new ConfModifier {
        def apply(c: Configuration) {
          c.set(testProp._1, overridenValue2)
        }
      }

      val inputPath = new File(getClass().getResource("/chain-input.txt").toURI).getAbsolutePath
      val input = TextInput[LongWritable, Text](inputPath)

      val outputPath = new File(new File(getClass().getResource("/").toURI), "mapReduceTest-output/confModifierIntegration").getAbsolutePath
      val output = TextOutput[LongWritable, Text](outputPath)


      val mapper1 = new ConfInterceptingMapper
      val mapper2 = new ConfInterceptingMapper
      val task1 = MapReduceTask(mapper1, "mock-task1")
      val task2 = MapReduceTask(mapper2, "mock-task2")

      val taskChain =
        conf -->
          input -->
          confModifier -->
          task1 -->
          confModifier2 -->
          task2 -->
          output  execute

      val actualOutput = Source.fromFile(new File(outputPath, "/part-r-00000")).mkString

      val expectedOutputPath = new File(getClass().getResource("/conf-modifier-integration-output.txt").toURI).getAbsolutePath
      val expectedOutput = Source.fromFile(expectedOutputPath).mkString

      actualOutput mustEqual expectedOutput
    }
  }
}

class ConfInterceptingMapper extends IdentityMapper[LongWritable, Text] {
  override def map(k: LongWritable, v: Text, context: ConfInterceptingMapper#ContextType) = {
    val confValue = context.getConfiguration.get("some-test-key")
    super.map(k, new Text(confValue), context)
  }
}
