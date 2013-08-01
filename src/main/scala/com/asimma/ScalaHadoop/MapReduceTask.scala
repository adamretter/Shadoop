package com.asimma.ScalaHadoop

import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import org.apache.hadoop.mapreduce.{Reducer => HReducer}
import org.apache.hadoop.mapreduce.Job

case class MapReduceTask[KIN, VIN, KOUT, VOUT](
  mapper: Mapper[KIN, VIN, _, _],
  combiner: Option[Reducer[_, _, _, _]],
  reducer: Option[Reducer[_, _, KOUT, VOUT]],
  name: String) {

  def initJob(conf: Configuration): Job = {
    val job = new Job(conf, this.name)
    job.setJarByClass(mapper.getClass)
    job.setMapperClass(mapper.getClass.asInstanceOf[Class[Mapper[KIN, VIN, _, _]]])

    combiner match {
      case Some(c) =>
        job.setCombinerClass(c.getClass.asInstanceOf[Class[Reducer[_, _, _, _]]])
      case None =>
    }

    reducer match {
      case Some(r) =>
        job.setReducerClass(r.getClass.asInstanceOf[Class[Reducer[_, _, KOUT, VOUT]]])
        job.setOutputKeyClass(r.kType)
        job.setOutputValueClass(r.vType)
      case None =>
        job.setOutputKeyClass(mapper.kType)
        job.setOutputValueClass(mapper.vType)
    }

    job
  }
}

/*
class MapOnlyTask[KIN, VIN, KOUT, VOUT](
  mapper: Mapper[KIN, VIN, _, _],
  name: String)
  extends MapReduceTask[KIN, VIN, KOUT, VOUT](mapper, None, None, name)    { }

class MapAndReduceTask[KIN, VIN, KOUT, VOUT](
  mapper: Mapper[KIN, VIN, _, _],
  reducer: Option[Reducer[_, _, KOUT, VOUT]],
  name: String)
  extends MapReduceTask[KIN, VIN, KOUT, VOUT](mapper, None, reducer, name)    { }

class MapCombineReduceTask[KIN, VIN, KOUT, VOUT](
  mapper: Mapper[KIN, VIN, _, _],
  combiner: Option[Reducer[_, _, _, _]],
  name: String)
  extends MapReduceTask[KIN, VIN, KOUT, VOUT](mapper, combiner, None, name)    { }
*/

object MapReduceTask {

  def apply[KIN, VIN, KOUT, VOUT](
   mapper: Mapper[KIN, VIN, KOUT, VOUT],
   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply(mapper, None, None, name)
  }

  def apply[KIN, VIN, KOUT, VOUT](
   mapper: Mapper[KIN, VIN, KOUT, VOUT],
   combiner: Reducer[_, _, _, _],
   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply(mapper, Option(combiner), None, name)
  }

  /*
  def apply[KIN, VIN, KOUT, VOUT, KTMP, VTMP](
   mapper: Mapper[KIN, VIN, KTMP, VTMP],
   reducer: Reducer[KTMP, VTMP, KOUT, VOUT],
   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply[KIN, VIN, KOUT, VOUT](mapper, None, Option(reducer), name)
  }*/

  def apply[KIN, VIN, KOUT, VOUT, KTMP, VTMP](
   mapper: Mapper[KIN, VIN, KTMP, VTMP],
   combiner: Reducer[_, _, _, _],
   reducer: Reducer[KTMP, VTMP, KOUT, VOUT],
   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = {
    apply[KIN, VIN, KOUT, VOUT](mapper, Option(combiner), Option(reducer), name)
  }
}
