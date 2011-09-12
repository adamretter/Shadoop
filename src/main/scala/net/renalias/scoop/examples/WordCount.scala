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
				 List((k, (0L /: v) ((total, next) => total+next)))
		}
	}

  def run(args: Array[String]) : Int = {
		TextInput[LongWritable, Text](args(0)) -->
		MapReduceTask(mapper, reducer, "Main task") -->
		TextOutput[Text, LongWritable](args(1)) execute

    return 0;
  }
}