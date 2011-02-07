package com.asimma.ScalaHadoop

import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib
import org.apache.hadoop.io._

object IO {

  class Input[K, V](
                     val dirName: String,
                     val inFormatClass: java.lang.Class[_ <: InputFormat[K, V]]
                     ) {}

  class Output[K, V](
                      val dirName: String,
                      val outFormatClass: java.lang.Class[_ <: lib.output.FileOutputFormat[K, V]]
                      ) {}


  /**This is a general class for inputs and outputs into the Map Reduce jobs.  Note that it's possible to
      write one type to a file and then have it be read as something else.  */

  class IO[KWRITE, VWRITE, KREAD, VREAD]
  (dirName: String,
   inFormatClass: Class[_ <: InputFormat[KREAD, VREAD]],
   outFormatClass: Class[lib.output.FileOutputFormat[KWRITE, VWRITE]]) {
    val input: Input[KREAD, VREAD] = new Input(dirName, inFormatClass)
    val output: Output[KWRITE, VWRITE] = new Output(dirName, outFormatClass)
  }


  def SeqFile[K, V](dirName: String)
                   (implicit mIn: Manifest[lib.input.SequenceFileInputFormat[K, V]],
                    mOut: Manifest[lib.output.SequenceFileOutputFormat[K, V]]) =
    new IO[K, V, K, V](dirName,
      mIn.erasure.asInstanceOf[Class[lib.input.FileInputFormat[K, V]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K, V]]])


  def Text[K, V](dirName: String)
                (implicit mIn: Manifest[lib.input.TextInputFormat],
                 mOut: Manifest[lib.output.TextOutputFormat[K, V]]) =
    new IO[K, V, LongWritable, Text](dirName,
      mIn.erasure.asInstanceOf[Class[lib.input.FileInputFormat[LongWritable, Text]]],
      mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K, V]]])

}