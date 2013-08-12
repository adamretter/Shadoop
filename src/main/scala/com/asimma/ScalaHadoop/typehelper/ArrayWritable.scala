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

import org.apache.hadoop.io.{ArrayWritable => HArrayWritable, Writable}
import scala.reflect.ClassTag

class ArrayWritable[T <: Writable](val values: Seq[T])(implicit val ctT: ClassTag[T])
  extends HArrayWritable(ctT.runtimeClass.asInstanceOf[Class[T]], values.toArray)
  with collection.mutable.Iterable[T] {

  override def toString = "[" + values.map(_.toString).reduceLeft(_ + ", " + _) + "]"

  def iterator = values.iterator

  /**
   * A copy of the ArrayWritable with a value prepended.
   */
  def +:(value: T) : ArrayWritable[T] = new ArrayWritable[T](value +: seq)

  /**
   * A copy of the ArrayWritable with a value appended.
   */
  def :+(value: T) : ArrayWritable[T] = new ArrayWritable[T](value :+ seq)
}

object ArrayWritable {

  def apply[T <: Writable](values: Seq[T])(implicit ctT: ClassTag[T]) = new ArrayWritable[T](values)

  implicit def ArrayWritableUnbox[T <: Writable](a: ArrayWritable[T]) : Seq[T] = a.toSeq

  implicit def ArrayWritableBox[T <: Writable](s: Seq[T])(implicit ctT: ClassTag[T]) = apply(s)
}