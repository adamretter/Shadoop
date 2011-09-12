package com.asimma.ScalaHadoop

import collection.JavaConversions._

import org.apache.hadoop.mapreduce.{Reducer => HReducer}

class Reducer[KIN, VIN, KOUT, VOUT](implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])
  extends HReducer[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] {

  type ContextType = HReducer[KIN, VIN, KOUT, VOUT]#Context
	type ReducerType = (KIN,Iterable[VIN]) => Iterable[(KOUT,VOUT)]

	// we wrap the reducer function in an option in case someone forgets to call reduceWith, and in that
	// case we'll return the empty iterable
	var reducer: Option[ReducerType] = None

  def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]]
  def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]]

	import scala.collection.JavaConversions._

	/**
	 * Run our reduce function, collect the output and save it into the context
	 */
	override def reduce(k: KIN, v: java.lang.Iterable[VIN], context: ContextType): Unit = {
    reducer.map(func => func(k, v).map(pair => context.write(pair._1, pair._2)))
	}

	/**
	 * Run our reduce function and return the raw output, may be useful during unit testing or troubleshooting
	 */
	def testF(k: KIN, v: java.lang.Iterable[VIN]) = {
		reducer.map(func => func(k, v))
	}

	/**
	 * Use this method to provide the function that will be used for the reducer
	 */
	def reduceWith(f:ReducerType) = reducer = Some(f)
}
