package com.asimma.ScalaHadoop

import org.apache.hadoop.mapreduce.{Reducer => HReducer}

abstract class Reducer[KIN, VIN, KOUT, VOUT](implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])
  extends HReducer[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] {

  type ContextType = HReducer[KIN, VIN, KOUT, VOUT]#Context

  def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]]

  def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]]

  var k: KIN = _
  var v: java.lang.Iterable[VIN] = _
  var context: ContextType = _

  override def reduce(k: KIN, v: java.lang.Iterable[VIN], context: ContextType): Unit = {
    this.k = k
    this.v = v
    this.context = context
    doReduce
  }

  def doReduce: Unit
}