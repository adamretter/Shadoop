/**
 * Copyright (C) 2013 Adam Retter (adam.retter@googlemail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asimma.ScalaHadoop.typehelper

import org.apache.hadoop.io.LongWritable

class LongArrayWritable(values: Seq[LongWritable]) extends ArrayWritable[LongWritable](values) with ArrayWritableType[LongWritable] {
  type ConcreteArrayWritable = LongArrayWritable

  def this() = this(Seq.empty[LongWritable])

  protected def make(values: Seq[LongWritable]) = new LongArrayWritable(values)
}

object LongArrayWritable extends AbstractArrayWritableObject[LongWritable] {
  type ConcreteArrayWritable = LongArrayWritable

  override def apply(values: Seq[LongWritable]) = new LongArrayWritable(values)
  override def apply(values: Array[LongWritable]) = new LongArrayWritable(values)
}
