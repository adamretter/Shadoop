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
package com.asimma.ScalaHadoop

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import java.io.StringWriter

/**
A class representing a bunch (one or more) of map and reduce operations, as well as
  all the additional parameters passed on to the Hadoop engine relating to the operations.

  <p>
  The expected usage is basically

  <pre>
  var a = Input("foo") --> Mapper1 --> Mapper2 --> Reducer --> Mapper3 --> Reducer2 --> Output("bar")
  a.execute
  </pre>
 */
class MapReduceTaskChain[KIN, VIN, KOUT, VOUT] extends Cloneable with Logging {

  type ConfModifier = MapReduceTaskChain.ConfModifier

  // The type of this chain link, to make some of the other functions more concise.
  type thisType = MapReduceTaskChain[KIN, VIN, KOUT, VOUT]

  /**A pointer to the previous node in the chain is null for the first link.  The type of prev is
  MapReduceTaskChain[_,_,K_FOR_MAP_TASK, V_FOR_MAP_TASK] but I don't want to introduce extra types into
  the parameters */
  var prev: MapReduceTaskChain[_, _, _, _] = null

  /**The task that we need to execute, the first try type parameters have to be equal to
      the last 2 type parameters of prev */
  var task: MapReduceTask[_, _, KOUT, VOUT] = null

  var conf: Configuration = null
  var confModifiers: List[ConfModifier] = List[ConfModifier]()
  var jobModifiers: List[JobModifier] = List[JobModifier]()

  val tmpDir: String = "tmp/tmp-" + MapReduceTaskChain.rand.nextLong()

  // TODO:  This is a type system disaster, but the alternatives are worse
  var defaultInput: IO.Input[KOUT,VOUT] = new IO.Input(tmpDir, classOf[lib.input.SequenceFileInputFormat[KOUT,VOUT]])
  var inputs: Array[IO.Input[KOUT,VOUT]] = Array()
  var output: IO.Output[KOUT,VOUT] = new IO.Output(tmpDir, classOf[lib.output.SequenceFileOutputFormat[KOUT,VOUT]])


  def cloneTypesafe(): thisType = clone().asInstanceOf[thisType]


  /**Returns a new MapReduceTaskChain that, in addition to performing everything specified by
     the current chain, also performs the MapReduceTask passed in */
  def -->[KOUT1, VOUT1](mrt: MapReduceTask[KOUT, VOUT, KOUT1, VOUT1]): MapReduceTaskChain[KIN, VIN, KOUT1, VOUT1] = {
    val chain = new MapReduceTaskChain[KIN, VIN, KOUT1, VOUT1]()
    chain.prev = this
    chain.task = mrt
    return chain
  }

  /**Adds a new link in the chain with a new node corresponding to executing that Map task */
  def -->[KOUT1, VOUT1](mapper: Mapper[KOUT, VOUT, KOUT1, VOUT1])
  : MapReduceTaskChain[KIN, VIN, KOUT1, VOUT1] = this --> MapReduceTask(mapper, "NO NAME")


  /**Add a confModifier to the current task by returning a copy of this chain
      with the confModifier pushed onto the confModifier list */
  def -->(confModifier: ConfModifier): thisType = {
    val chain = cloneTypesafe
    chain.confModifiers = confModifier :: chain.confModifiers
    return chain
  }

  /** Add a JobModifier to the current task by returning a copy of this chain
      with the JobModifier pushed onto the jobModifiers list */
  def -->(jobModifier: JobModifier) : thisType = {
    val chain = cloneTypesafe;
    chain.jobModifiers = jobModifier::chain.jobModifiers;
    return chain;
  }

  /**Adds an input source to the chain */
  def -->[K, V](in: IO.Input[K, V]): MapReduceTaskChain[KIN, VIN, K, V] = {
    val chain = new MapReduceTaskChain[KIN, VIN, K, V]()
    chain.prev = this
    chain.inputs = Array(in)
    return chain
  }

  /** Adds multiple input sources to the chain */
  def -->[K,V](inputs: Array[IO.Input[K,V]]): MapReduceTaskChain[KIN, VIN, K, V] = {
    val chain = new MapReduceTaskChain[KIN, VIN, K, V]();
    chain.prev = this;
    chain.inputs = inputs;
    return chain;
  }

  /**Adds an output sink to the chain */
  def -->(out: IO.Output[KOUT, VOUT]): thisType = {
    this.output = out
    return this
  }


  def execute(): Boolean = {
    if (prev != null) {
      info("Looking to previous Task for exec...")
      prev.execute()
    }

    if (task != null) {

      info(s"Executing task '${task.name}'...")

      val conf = getConf
      // Off the bat, apply the modifications from all the ConfModifiers we have queued up at this node.
      confModifiers map ((mod: ConfModifier) => mod(conf))

      val job = task initJob conf

      // Apply the modifications from all the JobModifiers we have queued up at this node.
      jobModifiers foreach ((mod: JobModifier) => mod(job))

      job setOutputFormatClass(output.outFormatClass)

      if (classOf[lib.output.FileOutputFormat[KOUT, VOUT]].isAssignableFrom(output.outFormatClass)) {
        lib.output.FileOutputFormat.setOutputPath(job, new Path(output.dirName))
      }

      if (prev.inputs.isEmpty) {
        job setInputFormatClass    prev.defaultInput.inFormatClass
        if (classOf[lib.input.FileInputFormat[KIN, VIN]].isAssignableFrom(prev.defaultInput.inFormatClass)
          || classOf[PathInputFormat[KIN, VIN]].isAssignableFrom(prev.defaultInput.inFormatClass)) {
          info("Adding input path: " + prev.defaultInput.dirName)
          lib.input.FileInputFormat.addInputPath(job, new Path(prev.defaultInput.dirName))
        }
      } else {
        job setInputFormatClass   prev.inputs(0).inFormatClass
        prev.inputs.foreach ((io) => {
          if (classOf[lib.input.FileInputFormat[KIN, VIN]].isAssignableFrom(prev.inputs(0).inFormatClass)
            || classOf[PathInputFormat[KIN, VIN]].isAssignableFrom(prev.inputs(0).inFormatClass)) {
            info("Adding input path: " + io.dirName)
            lib.input.FileInputFormat.addInputPath(job, new Path(io.dirName))
          }
        })
      }

      debug(debugJob(job))
      debug(debugConfig(job.getConfiguration))

      job waitForCompletion true
      return true
    }

    return true
  }

  private def debugConfig(config: Configuration, asJson: Boolean = false) = {
    val writer = new StringWriter()
    if(asJson) {
      Configuration.dumpConfiguration(config, writer)
    } else {
      config.writeXml(writer)
    }
    "ConfigDebug = " + writer.toString
  }

  private def debugJob(job: Job) : String = {
    import scala.reflect.runtime.{universe => ru}
    import scala.reflect.runtime.universe._
    import scala.reflect.ClassTag

    def getFields[T: TypeTag] = typeOf[T].members.collect {
      case t: TermSymbol if t.isVal || t.isVar =>
        t
    }.toList

    def fieldsNameValue[T](instance: T)(implicit ctT : ClassTag[T], ttT: TypeTag[T]) : Seq[(String, String)]= {
      val m = ru.runtimeMirror(job.getClass.getClassLoader)
      val ref = m.reflect(instance)
      getFields[T].map(field => {
        val fieldMirror = ref.reflectField(field)
        (field.fullName,  Option(fieldMirror.get).getOrElse("null").toString)
      })
    }

    "Job Debug = " +
      fieldsNameValue(job).map(kv => s"${kv._1}: ${kv._2}").mkString(System.getProperty("line.separator"))
  }

  def getConf: Configuration = if (conf == null) prev.getConf else conf
}

object MapReduceTaskChain {
  val rand = new scala.util.Random()

  // Generic parameter setter
  trait ConfModifier {
    def apply(c: Configuration): Unit
  }

  class SetParam(val param: String, val value: String) extends ConfModifier {
    def apply(c: Configuration) = {
      c.set(param, value)
    }
  }

  def Param(p: String, v: String) = new SetParam(p, v)

  def apply(conf: Configuration): MapReduceTaskChain[None.type, None.type, None.type, None.type] = {
    val c = new MapReduceTaskChain[None.type, None.type, None.type, None.type]()
    c.conf = conf
    return c
  }

  def init: MapReduceTaskChain[None.type, None.type, None.type, None.type] = apply(new Configuration)

  // this allows us to use "input --> mapper --> reducer --> out"
  // TODO: to check how to allow types that subclass IO.Input
  implicit def -->[K,V](in: IO.Input[K,V]) = {
    MapReduceTaskChain.init --> in
  }

  // this allows us to use "Array[input] --> mapper --> reducer --> out"
  implicit def -->[K,V](in: Array[IO.Input[K,V]]) = {
    MapReduceTaskChain.init --> in
  }


  class SetPartitioner(val partitionerClass: java.lang.Class[_ <: org.apache.hadoop.mapreduce.Partitioner[_, _]]) extends JobModifier {
    def apply(job: Job) : Unit = { job.setPartitionerClass(partitionerClass); }
  }
  def Partitioner(partitionerClass: java.lang.Class[_ <: org.apache.hadoop.mapreduce.Partitioner[_, _]]) =
    new SetPartitioner(partitionerClass);

  class SetNumReduceTasks(val numReduceTasks: Int) extends JobModifier {
    def apply(job: Job) : Unit = { job.setNumReduceTasks(numReduceTasks); }
  }
  def NumReduceTasks(numReduceTasks: Int) = new SetNumReduceTasks(numReduceTasks);
}

// Expose setX() methods on the Job object via JobModifiers
trait JobModifier {
  def apply(job: Job) : Unit;
}
