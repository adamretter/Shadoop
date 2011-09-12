package com.asimma.ScalaHadoop

import collection.JavaConversions._

import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import scala.Some

abstract class Mapper[KIN, VIN, KOUT, VOUT](implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])
  extends HMapper[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] {

  type ContextType = HMapper[KIN, VIN, KOUT, VOUT]#Context

	type MapperType = (KIN,VIN) => Iterable[(KOUT,VOUT)]
	var mapper: Option[MapperType] = None

  def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]]
  def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]]

	override def map(k: KIN, v: VIN, context: ContextType): Unit = {
		mapper.map(func => func(k, v).map(pair => context.write(pair._1, pair._2)))
  }

	def mapWith(f:MapperType) = mapper = Some(f)
}