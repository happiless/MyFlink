package com.happiless.flink

import java.net.URL

import org.apache.flink.api.scala.ExecutionEnvironment

object FlinkBatchWordCount {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val url: URL = getClass.getResource("/data/words")
    val data: DataSet[String] = env.readTextFile(url.getPath)
    data.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()

  }

}
