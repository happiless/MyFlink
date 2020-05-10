package com.sxt.flink.transformation

import com.sxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestUnion {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream1: DataStream[(String, Int)] = streamEnv.fromElements(("a", 1),("b", 2),("c", 3))
    val stream2: DataStream[(String, Int)] = streamEnv.fromElements(("d", 4),("e", 5),("f", 6))
    val stream3: DataStream[(String, Int)] = streamEnv.fromElements(("g", 7),("h", 8),("i", 9))

    val result: DataStream[(String, Int)] = stream1.union(stream2, stream3)
    result.print()
    streamEnv.execute("union")
  }

}
