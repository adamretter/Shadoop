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
package shadoop.typehelper

import org.apache.hadoop.io.Writable
import java.io.{DataInput, DataOutput}

sealed trait HListWritable

final case class ::[+H <: Writable, +T <: HListWritable](head: H, tail: T) extends HListWritable with Writable {

  import HListWritable.hlistWritableOps

  override def toString = s"$head :: ${tail.toString}"

  def length = {
    var l = 0
    this.applyDown {
      item =>
        l = l + 1
    }
    l
  }

  override def readFields(in: DataInput) {
    this.applyDown {
      item =>
        item.readFields(in)
    }
  }

  override def write(out: DataOutput): Unit = {
    this.applyDown {
      item =>
        item.write(out)
    }
  }
}

final class HListWritableOps[+L <: HListWritable](l: L) {

  def ::[H <: Writable](h : H) : H :: L = shadoop.typehelper.::(h, l)

  def applyDown(f: Writable => Unit): Unit = {
    l match {

      case head :: tail => {
        f(head)
        tail.applyDown(f)
      }

      case _: HListWritableNil =>
        //do nothing
    }
  }
}

sealed trait HListWritableNil extends HListWritable {
  def ::[H <: Writable](h: H) = shadoop.typehelper.::(h, this)
}

case object HListWritableNil extends HListWritableNil

object HListWritable {

  def apply() = HListWritableNil

  implicit def hlistWritableOps[L <: HListWritable](l : L) : HListWritableOps[L] = new HListWritableOps(l)
}
