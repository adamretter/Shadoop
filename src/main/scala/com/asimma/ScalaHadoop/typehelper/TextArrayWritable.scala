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

import org.apache.hadoop.io.Text

class TextArrayWritable(override val values: Seq[Text]) extends ArrayWritable[Text](values) with ArrayWritableType[Text] {
  type ConcreteArrayWritable = TextArrayWritable

  def this() = this(Seq.empty[Text])

  protected def make(values: Seq[Text]) = new TextArrayWritable(values)
}

object TextArrayWritable extends AbstractArrayWritableObject[Text] {
  type ConcreteArrayWritable = TextArrayWritable

  override def apply(values: Seq[Text]) = new TextArrayWritable(values)
}
