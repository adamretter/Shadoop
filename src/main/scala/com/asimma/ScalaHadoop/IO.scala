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

import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib
import org.apache.hadoop.io.{LongWritable, Text}

object IO {

  class Input[K, V](val dirName: String,
                    val inFormatClass: java.lang.Class[_ <: InputFormat[K, V]])

  class Output[K, V](val dirName: String,
                     val outFormatClass: java.lang.Class[_ <: lib.output.FileOutputFormat[K, V]])

  /**This is a general class for inputs and outputs into the Map Reduce jobs.  Note that it's possible to
      write one type to a file and then have it be read as something else.  */

  class IO[KWRITE, VWRITE, KREAD, VREAD]
  (dirName: String,
   inFormatClass: Class[_ <: InputFormat[KREAD, VREAD]],
   outFormatClass: Class[lib.output.FileOutputFormat[KWRITE, VWRITE]]) {
    val input: Input[KREAD, VREAD] = new Input(dirName, inFormatClass)
    val output: Output[KWRITE, VWRITE] = new Output(dirName, outFormatClass)
  }


  def SeqFile[K, V](dirName: String)(implicit mIn: Manifest[lib.input.SequenceFileInputFormat[K, V]],
                                     mOut: Manifest[lib.output.SequenceFileOutputFormat[K, V]]) =
    new IO[K, V, K, V](dirName,
      mIn.erasure.asInstanceOf[Class[lib.input.FileInputFormat[K, V]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K, V]]])


  def Text[K, V](dirName: String)(implicit mIn: Manifest[lib.input.TextInputFormat],
                                  mOut: Manifest[lib.output.TextOutputFormat[K, V]]) =
    new IO[K, V, LongWritable, Text](dirName,
      mIn.erasure.asInstanceOf[Class[lib.input.FileInputFormat[LongWritable, Text]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K, V]]])

  def MultiSeqFile[K,V](dirNames : Array[String])
                       (implicit mIn:   Manifest[lib.input.SequenceFileInputFormat[K,V]],
                        mOut:  Manifest[lib.output.SequenceFileOutputFormat[K,V]]) =
    dirNames.map(new IO[K,V,K,V](_,
      mIn .erasure.asInstanceOf[Class[lib.input.FileInputFormat[K,V]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K,V]]]));


  def MultiText[K,V](dirNames : Array[String])
                    (implicit mIn:   Manifest[lib.input.TextInputFormat],
                     mOut:  Manifest[lib.output.TextOutputFormat[K,V]]) =
    dirNames.map(new IO[K,V,LongWritable,Text](_,
      mIn .erasure.asInstanceOf[Class[lib.input.FileInputFormat[LongWritable,Text]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K,V]]]));
}

/**
 * syntactic sugar
 *
 * TODO: implement the same for IO.SeqFile
 */
object TextInput {
	def apply[K,V](folder: String)(implicit mIn: Manifest[lib.input.TextInputFormat],
                                  mOut: Manifest[lib.output.TextOutputFormat[K, V]]) = {
   IO.Text[K,V](folder).input
	}
}

object TextOutput {
	def apply[K,V](folder: String)(implicit mIn: Manifest[lib.input.TextInputFormat],
                                  mOut: Manifest[lib.output.TextOutputFormat[K, V]]) = {
   IO.Text[K,V](folder).output
	}
}

object SeqFileInput {
  def apply[K,V](folder: String)(implicit mIn: Manifest[lib.input.SequenceFileInputFormat[K,V]],
                                 mOut: Manifest[lib.output.SequenceFileOutputFormat[K, V]]) = {
    IO.SeqFile[K,V](folder).input
  }
}

object SeqFileOutput {
  def apply[K,V](folder: String)(implicit mIn: Manifest[lib.input.SequenceFileInputFormat[K,V]],
                                 mOut: Manifest[lib.output.SequenceFileOutputFormat[K, V]]) = {
    IO.SeqFile[K,V](folder).output
  }
}