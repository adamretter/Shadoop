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
import com.asimma.ScalaHadoop.IO.{Input, Output}
import com.asimma.ScalaHadoop.MapReduceTaskChain._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class MapReduceTaskChainSpec extends Specification {

  "conf -->" should {

    "set the correct initial config" in {

      val testProp = ("some-test-key", "some-test-value")

      val conf = new Configuration()
      conf.set(testProp._1, testProp._2)

      val taskChain = -->(conf)

      taskChain.getEffectiveConf.get(testProp._1) mustEqual testProp._2
    }

    "be inherited to the input" in {

      val testProp = ("some-test-key", "some-test-value")

      val conf = new Configuration()
      conf.set(testProp._1, testProp._2)

      val mockInput = new Input[Text, Text]("some-dir", classOf[InputFormat[Text, Text]])
      val taskChain = conf --> mockInput

      taskChain.getEffectiveConf.get(testProp._1) mustEqual testProp._2
    }

    "be inherited to the first task" in {

      val testProp = ("some-test-key", "some-test-value")

      val conf = new Configuration()
      conf.set(testProp._1, testProp._2)

      val mockInput = new Input[Text, Text]("some-dir", classOf[InputFormat[Text, Text]])
      val mockTask = new MapReduceTask[Text, Text, Text, Text](None, None, None, "mock-task")
      val taskChain = conf --> mockInput --> mockTask

      taskChain.getEffectiveConf.get(testProp._1) mustEqual testProp._2
    }

    "be inherited to the output" in {
      val testProp = ("some-test-key", "some-test-value")

      val conf = new Configuration()
      conf.set(testProp._1, testProp._2)

      val mockInput = new Input[Text, Text]("some-dir", classOf[InputFormat[Text, Text]])
      val mockTask = new MapReduceTask[Text, Text, Text, Text](None, None, None, "mock-task")
      val mockOutput = new Output[Text, Text]("some-dir", classOf[FileOutputFormat[Text, Text]])
      val taskChain = conf --> mockInput --> mockTask --> mockOutput

      taskChain.getEffectiveConf.get(testProp._1) mustEqual testProp._2
    }
  }

  "confModifier --> " should {

    "override the default config" in {
      val testProp = ("some-test-key", "some-test-value")
      val overridenValue = "overriden-test-value"

      val conf = new Configuration()
      conf.set(testProp._1, testProp._2)

      val confModifier = new ConfModifier {
        def apply(c: Configuration) {
          c.set(testProp._1, overridenValue)
        }
      }

      val taskChain = -->(conf) --> confModifier

      taskChain.getEffectiveConf.get(testProp._1) mustEqual overridenValue
    }

    "be inherited into input" in {
      val testProp = ("some-test-key", "some-test-value")
      val overridenValue = "overriden-test-value"

      val conf = new Configuration()
      conf.set(testProp._1, testProp._2)

      val confModifier = new ConfModifier {
        def apply(c: Configuration) {
          c.set(testProp._1, overridenValue)
        }
      }

      val mockInput = new Input[Text, Text]("some-dir", classOf[InputFormat[Text, Text]])
      val taskChain = -->(conf) --> confModifier --> mockInput

      taskChain.getEffectiveConf.get(testProp._1) mustEqual overridenValue
    }

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

      val mockInput = new Input[Text, Text]("some-dir", classOf[InputFormat[Text, Text]])
      val mockTask = new MapReduceTask[Text, Text, Text, Text](None, None, None, "mock-task")

      val taskChainPart1 = -->(conf) --> confModifier --> mockInput
      val taskChainPart2 = taskChainPart1 --> confModifier2 --> mockTask

      taskChainPart1.getEffectiveConf.get(testProp._1) mustEqual overridenValue
      taskChainPart2.getEffectiveConf.get(testProp._1) mustEqual overridenValue2
    }
  }
}