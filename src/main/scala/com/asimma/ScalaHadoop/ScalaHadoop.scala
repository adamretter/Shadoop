package com.asimma.ScalaHadoop

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{ToolRunner, Tool}

abstract class ScalaHadoop extends Configured with Tool {
  def main(args: Array[String]) = ToolRunner.run(this, args)
}








