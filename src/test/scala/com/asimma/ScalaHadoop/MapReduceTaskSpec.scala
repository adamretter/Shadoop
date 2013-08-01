package com.asimma.ScalaHadoop

import ImplicitConversion._
import org.specs2.mutable._
import org.apache.hadoop.io._

class MapReduceTaskSpec extends Specification {

  type M = Mapper[Text, LongWritable, Text, LongWritable]

  type R = Reducer[Text, LongWritable, Text, LongWritable]

  object MapperTask extends M {
    mapWith {
      (k,v) =>
        List((k, 1L))
    }
//    def doMap {
//      context.write(k, 1L)
//    }
  }

  object ReduceTask extends R {
    reduceWith {
      (k,v) =>
        List((k, v.reduceLeft(_ + _)))
    }

//    def doReduce {
//      context.write(k, v.reduceLeft(_ + _))
//    }
  }

  "MapReduceTask" should {
    val task = MapReduceTask(MapperTask, ReduceTask, "Name")
    "apply" in {
      task.name mustEqual "Name"
      task.mapper.asInstanceOf[M] mustEqual MapperTask
      task.reducer.get.asInstanceOf[R] mustEqual ReduceTask
    }
  }
}