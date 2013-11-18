# Shadoop

A Hadoop DSL and lightweight wrapper for Scala


[![Build Status](https://travis-ci.org/adamretter/Shadoop.png?branch=master)](https://travis-ci.org/adamretter/Shadoop)

This fork of ScalaHadop is mostly just cherry-picked commits from the forks by [@hito-asa](https://github.com/hiti-asa/ScalaHadoop), [@ivmaykov](https://github.com/ivmaykov/ScalaHadoop) and [@oscarrenalis](https://github.com/oscarrenalias/ScalaHadoop), of the original work by [@bsdfish](https://github.com/bsdfish/ScalaHadoop). In addition there are a few extra features and a cleaned up Maven build.

This code provides some syntactic sugar on top of Hadoop in order to make
it more usable from Scala.  Take a look at src/main/scala/net/renalias/scoop/examples/WordCount.scala for more
details.

## License
[Apache License, Version 2.0](http://opensource.org/licenses/Apache-2.0)

## Usage
### Basic Usage
A basic mapper looks like:

```scala
val mapper = new Mapper[LongWritable, Text, Text, LongWritable] {
    mapWith {
        (k, v) =>
            (v split " |\t").map(x => (new Text(x), new LongWritable(1L))).toList
    }
}
```

a reducer looks like this:

```scala
val reducer = new Reducer[Text, LongWritable, Text, LongWritable] {
    reduceWith {
        (k, v) =>
            List((k, (0L /: v)((total, next) => total + next)))
    }
}
```

and, the pipeline to bind them together may look like this:

```scala
TextInput[LongWritable, Text]("/tmp/input.txt") -->
MapReduceTask(mapper, reducer, "Word Count")    -->
TextOutput[Text, LongWritable]("/tmp/output")   execute
```


The key difference here between standard mappers and reducers is that the map and reduce parts are written as side-effect
free functions that accept a key and a value, and return an iterable; code behind the scenes will take care of
updating Hadoop's Context object.

Some note still remains to be done to polish the current interface, to remove things like .toList from the mapper and
the creation of Hadoop's specific Text and LongWritable objects.

Note that implicit conversion is used to convert between LongWritable and longs, as well as Text
and Strings.  The types of the input and output parameters only need to be stated as the
generic specializers of the class it extends.

These mappers and reducers can be chained together with the --> operator:

```scala
object WordCount extends ScalaHadoop {
  def run(args: Array[String]) : Int = {
    TextInput[LongWritable, Text](args(0)) -->
    MapReduceTask(mapper, reducer, "Main task") -->
    TextOutput[Text, LongWritable](args(1)) execute

    0 //result code
  }
}
```

### Multiple map/reduce
Multiple map/reduce runs may be chained together:

```scala
object WordsWithSameCount extends ScalaHadoop {
  def run(args: Array[String]) : Int = {
    TextInput[LongWritable, Text](args(0)) -->
    MapReduceTask(tokenizerMap1, sumReducer, "Sum") -->
    MapReduceTask(flipKeyValueMap, wordListReducer, "Reduce") -->
    TextOutput[LongWritable, Text](args(1)) execute

    0 //result code
  }
}
```

## Contributors
- **Alex Simma**: Developer of original version of ScalaHadoop. https://github.com/bsdfish/ScalaHadoop
- **ASAI Hitoshi**: Cherry-picked - Code re-organisation and initial Maven build. https://github.com/hiti-asa/ScalaHadoop
- **Ilya Maykov**: Cherry-picked - Various fixes, and support for Multiple Input Paths. https://github.com/ivmaykov/ScalaHadoop
- **Oscar Renalias**: Cherry-picked - Scala Syntax improvements. https://github.com/oscarrenalias/ScalaHadoop
- **Rob Walpole**: Various bug fixes: https://github.com/rwalpole/ScalaHadoop
