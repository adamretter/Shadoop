/**
 * Copyright 2013
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asimma.ScalaHadoop

import collection.JavaConversions._

import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import scala.Some

abstract class Mapper[KIN, VIN, KOUT, VOUT](implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])
  extends HMapper[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] with MapReduceConfig {

  type ContextType = HMapper[KIN, VIN, KOUT, VOUT]#Context

	type MapperType = (KIN,VIN) => Iterable[(KOUT,VOUT)]
	var mapper: Option[MapperType] = None

  def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]]
  def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]]

	override def map(k: KIN, v: VIN, context: ContextType): Unit = {
		mapper.map(func => func(k, v).map(pair => context.write(pair._1, pair._2)))
  }

  override def setup(context: ContextType) {
    this.configuration = Option(context.getConfiguration)
  }

	def mapWith(f:MapperType) = mapper = Some(f)
}