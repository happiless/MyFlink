package com.sxt.flink.transformation

import com.sxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.Seq

object TestSplit {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)
    val result: SplitStream[StationLog] = stream.split(fun => {
      if (fun.callType.equals("success")) Seq("Success") else Seq("Not Success")
    })
    //根据标签得到不同流
    val stream1: DataStream[StationLog] = result.select("Success")
    val stream2: DataStream[StationLog] = result.select("Not Success")
    stream1.print("通话成功")
    stream2.print("通话不成功")
    streamEnv.execute()
  }

}
