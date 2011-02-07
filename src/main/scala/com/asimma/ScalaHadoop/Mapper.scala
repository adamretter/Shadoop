package com.asimma.ScalaHadoop

import org.apache.hadoop.mapreduce.{Mapper => HMapper}

abstract class Mapper[KIN, VIN, KOUT, VOUT](implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])
  extends HMapper[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] {

  type ContextType = HMapper[KIN, VIN, KOUT, VOUT]#Context

  def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]]

  def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]]

  var k: KIN = _
  var v: VIN = _
  var context: ContextType = _

  override def map(k: KIN, v: VIN, context: ContextType): Unit = {
    this.k = k
    this.v = v
    this.context = context
    doMap
  }

  def doMap: Unit
}
