package com.asimma.ScalaHadoop

import ImplicitConversion._
import org.specs.SpecificationWithJUnit
import org.apache.hadoop.io._

class MapReduceTaskSpec extends SpecificationWithJUnit {

  type M = Mapper[Text, LongWritable, Text, LongWritable]

  type R = Reducer[Text, LongWritable, Text, LongWritable]

  object MapperTask extends M {
    def doMap {
      context.write(k, 1L)
    }
  }

  object ReduceTask extends R {
    def doReduce {
      context.write(k, v.reduceLeft(_ + _))
    }
  }

  "MapReduceTask" should {
    "apply" in {
      val task = MapReduceTask(MapperTask, ReduceTask, "Name")
      task.name mustBe "Name"
      task.mapper.asInstanceOf[M] mustEq MapperTask
      task.reducer.get.asInstanceOf[R] mustEq ReduceTask
    }
  }
}