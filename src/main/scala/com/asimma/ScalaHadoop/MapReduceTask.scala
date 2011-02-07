package com.asimma.ScalaHadoop

import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import org.apache.hadoop.mapreduce.{Reducer => HReducer}
import org.apache.hadoop.mapreduce.Job

case class MapReduceTask[KIN, VIN, KOUT, VOUT](
                                                mapper: Mapper[KIN, VIN, _, _],
                                                reducer: Option[Reducer[_, _, KOUT, VOUT]],
                                                name: String) {

  def initJob(conf: Configuration): Job = {
    val job = new Job(conf, this.name)
    job.setJarByClass(mapper.getClass)
    job.setMapperClass(mapper.getClass.asInstanceOf[Class[HMapper[KIN, VIN, _, _]]])
    reducer match {
      case Some(r) =>
        job.setReducerClass(r.getClass.asInstanceOf[Class[HReducer[_, _, KOUT, VOUT]]])
        job.setOutputKeyClass(r.kType)
        job.setOutputValueClass(r.vType)
      case None =>
        job.setOutputKeyClass(mapper.kType)
        job.setOutputValueClass(mapper.vType)
    }
    job
  }

}

object MapReduceTask {

  def apply[KIN, VIN, KOUT, VOUT](
                                   mapper: Mapper[KIN, VIN, KOUT, VOUT],
                                   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply(mapper, None, name)
  }

  def apply[KIN, VIN, KOUT, VOUT, KTMP, VTMP](
                                               mapper: Mapper[KIN, VIN, KTMP, VTMP],
                                               reducer: Reducer[KTMP, VTMP, KOUT, VOUT],
                                               name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply[KIN, VIN, KOUT, VOUT](mapper, Option(reducer), name)
  }
}