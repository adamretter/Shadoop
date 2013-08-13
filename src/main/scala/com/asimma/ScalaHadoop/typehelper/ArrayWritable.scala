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
package com.asimma.ScalaHadoop.typehelper

import org.apache.hadoop.io.{ArrayWritable => HArrayWritable, Writable, Text, LongWritable}
import scala.reflect.ClassTag

trait ArrayWritableType[T <: Writable] {
  type ConcreteArrayWritable <: ArrayWritable[T]
}

trait AbstractArrayWritableObject[T <: Writable] extends ArrayWritableType[T] {
  implicit def ArrayWritableUnbox(a: ConcreteArrayWritable) : Seq[T] = a.toSeq
  implicit def ArrayWritableBox(s: Seq[T]) = apply(s)
  def apply(values: Seq[T])
}

abstract class ArrayWritable[T <: Writable] (val values: Seq[T])(implicit val ctT: ClassTag[T])
  extends HArrayWritable(ctT.runtimeClass.asInstanceOf[Class[T]], values.toArray)
  with ArrayWritableType[T] with collection.mutable.Iterable[T] {

  //type ConcreteArrayWritable <: ArrayWritable[T]

  protected def make(values: Seq[T]) : ConcreteArrayWritable

  def iterator = values.iterator

  /**
   * A copy of the ArrayWritable with a value prepended.
   */
  def +:(value: T) : ConcreteArrayWritable = make(value +: values)

  /**
   * A copy of the ArrayWritable with a value appended.
   */
  def :+(value: T) : ConcreteArrayWritable  = make(value +: values)

  override def toString = "[" + values.map(_.toString).foldLeft("")(_ + ", " + _) + "]"
}