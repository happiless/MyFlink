package com.sxt.flink.transformation

import com.sxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestTransformation {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)
    stream.filter(_.callType.equals("success"))
      .map(log =>  (log.sid, log.duration))
      .keyBy(0)
      .reduce((t1, t2)=>{
        val duration = t1._2 + t2._2
        (t1._1, duration)
      }).print()
    streamEnv.execute("reduce")
  }

}
