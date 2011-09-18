# ScalaHadoop

This code provides some syntactic sugar on top of Hadoop in order to make
it more usable from Scala.  Take a look at Examples.scala for more
details.

## License
Apache License, Version 2.0

## Usage
### Basic Usage
A basic mapper looks like

    val mapper = new Mapper[LongWritable, Text, Text, LongWritable] {
		mapWith { (k, v) =>
				(v split " |\t").map(x => (new Text(x), new LongWritable(1L))).toList
		}
	}

and a reducer

    val reducer = new Reducer[Text, LongWritable, Text, LongWritable] {
		reduceWith { (k, v) =>
				List((k, (0L /: v)((total, next) => total + next)))
		}
	}

The key difference here between standard mappers and reducers is that the map and reduce parts are written as side-effect
free functions that accept a key and a value, and return an iterable; code behind the scenes will take care of
updating Hadoop's Context object.

Some note still remains to be done to polish the current interface, to remove things like .toList from the mapper and
the creation of Hadoop's specific Text and LongWritable objects.

Note that implicit conversion is used to convert between LongWritable and longs, as well as Text
and Strings.  The types of the input and output parameters only need to be stated as the
generic specializers of the class it extends.

These mappers and reducers can be chained together with the --> operator 

    object WordCount extends ScalaHadoopTool{ 
      def run(args: Array[String]) : Int = {  
        TextInput[LongWritable, Text](args(0)) -->
		MapReduceTask(mapper, reducer, "Main task") -->
		TextOutput[Text, LongWritable](args(1)) execute

        return 0;
      }
    }

### Multiple map/reduce
Multiple map/reduce runs can be chained together

    object WordsWithSameCount extends ScalaHadoopTool {
      def run(args: Array[String]) : Int = {
        IO.Text[LongWritable, Text](args(0)).input                    -->  
        MapReduceTask.MapReduceTask(TokenizerMap1, SumReducer)        -->
        MapReduceTask.MapReduceTask(FlipKeyValueMap, WordListReducer) -->
        IO.Text[LongWritable, Text](args(1)).output) execute;
        return 0;
      }
    }

## Contributor
**Alex Simma** : Developer of original version of ScalaHadoop.

