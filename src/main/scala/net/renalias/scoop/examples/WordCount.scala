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
package net.renalias.scoop.examples

import com.asimma.ScalaHadoop._
import com.asimma.ScalaHadoop.MapReduceTaskChain._
import org.apache.hadoop.io.{LongWritable, Text}

object WordCount extends ScalaHadoop {

	import com.asimma.ScalaHadoop.ImplicitConversion._
	import com.asimma.ScalaHadoop.MapReduceTask._

	// TODO: there should be no need to explicitly create Text and LongWritable object because there are implicit
	// conversions in place...
	val mapper = new Mapper[LongWritable, Text, Text, LongWritable] {
		mapWith { (k,v) =>
			(v split " |\t").map(x=>(new Text(x), new LongWritable(1L))).toList
		}
	}

	// TODO: could we use an implicit conversion to convert from Tuple2 to List[Tuple2] with one item only?
	val reducer = new Reducer[Text, LongWritable, Text, LongWritable] {
		reduceWith { (k,v) =>

      //System.err.println(s"REDUCER=($k, $v)")

				 List(
           (
             k,
             (0L /: v) ((total, next) => total+next)
           )

         )
		}
	}

  def run(args: Array[String]) : Int = {
		TextInput[LongWritable, Text]("file:///tmp/wordcount-input") -->
		MapReduceTask(mapper, reducer, reducer, "Main task") -->
		TextOutput[Text, LongWritable]("file:///tmp/scala") execute

    return 0;
  }
}