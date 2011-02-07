package com.asimma.ScalaHadoop

import ImplicitConversion._
import org.specs.SpecificationWithJUnit
import org.apache.hadoop.io._

class MapReduceTaskSpec extends SpecificationWithJUnit {

  object MapperTask extends Mapper[Text, LongWritable, Text, LongWritable] {
    def doMap {
      context.write(k, 1L)
    }
  }

  object ReduceTask extends Reducer[Text, LongWritable, Text, LongWritable] {
    def doReduce {
      context.write(k, v.reduceLeft(_ + _))
    }
  }

  "MapReduceTask" should {
    "apply" in {
      val task = MapReduceTask(MapperTask, ReduceTask, "Name")
      task.name mustBe "Name"
      task.mapper mustEq MapperTask
      task.reducer mustEq ReduceTask
    }
  }
}