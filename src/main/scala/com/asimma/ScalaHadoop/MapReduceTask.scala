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

import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import org.apache.hadoop.mapreduce.{Reducer => HReducer}
import org.apache.hadoop.mapreduce.Job

case class MapReduceTask[KIN, VIN, KOUT, VOUT](mapper: Option[Mapper[KIN, VIN, _, _]],
                                               combiner: Option[Reducer[_, _, _, _]],
                                               reducer: Option[Reducer[_, _, KOUT, VOUT]],
                                               name: String) {

  def initJob(conf: Configuration): Job = {
    val job = new Job(conf, this.name)

    //job.setJarByClass(mapper.getClass)

    mapper match {
      case Some(m) =>
        job.setMapperClass(m.getClass.asInstanceOf[Class[HMapper[KIN, VIN, _, _]]])
      case None =>
    }

    combiner match {
      case Some(c) =>
        job.setCombinerClass(c.getClass.asInstanceOf[Class[HReducer[_, _, _, _]]])
      case None =>
    }

    reducer match {
      case Some(r) =>
        job.setReducerClass(r.getClass.asInstanceOf[Class[HReducer[_, _, KOUT, VOUT]]])
        job.setOutputKeyClass(r.kType)
        job.setOutputValueClass(r.vType)

        mapper match {
          case Some(m) =>
            job.setMapOutputKeyClass(m.kType)
            job.setMapOutputValueClass(m.vType)
          case None =>
            job.setMapOutputKeyClass(r.kinType)
            job.setMapOutputValueClass(r.vinType)
        }

      case None if(!mapper.isEmpty) =>
        job.setOutputKeyClass(mapper.get.kType)
        job.setOutputValueClass(mapper.get.vType)
    }

    job
  }
}

object MapReduceTask {

  def apply[KIN, VIN, KOUT, VOUT](
   mapper: Mapper[KIN, VIN, KOUT, VOUT],
   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply(Option(mapper), None, None, name)
  }

  def apply[KIN, VIN, KOUT, VOUT](
   mapper: Mapper[KIN, VIN, KOUT, VOUT],
   combiner: Reducer[_, _, _, _],
   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply(Option(mapper), Option(combiner), None, name)
  }

  def apply[KIN, VIN, KOUT, VOUT, KTMP, VTMP](
   mapper: Mapper[KIN, VIN, KTMP, VTMP],
   combiner: Reducer[_, _, _, _],
   reducer: Reducer[KTMP, VTMP, KOUT, VOUT],
   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply[KIN, VIN, KOUT, VOUT](Option(mapper), Option(combiner), Option(reducer), name)
  }

  def apply[KIN, VIN, KOUT, VOUT](
     reducer: Reducer[KIN, VIN, KOUT, VOUT],
     name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply[KIN, VIN, KOUT, VOUT](None, None, Option(reducer), name)
  }
}
