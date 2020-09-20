package com.happiless.flink.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestConnect {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream1: DataStream[(String, Int)] = streamEnv.fromElements(("a", 1),("b", 2),("c", 3))
    val stream2: DataStream[String] = streamEnv.fromElements("e", "f", "g")
    val stream3: ConnectedStreams[(String, Int), String] = stream1.connect(stream2)
    stream3.map(
      f=>{f}
      ,
      f=>{(f, 1)}
    ).print()
    streamEnv.execute()
  }

}
