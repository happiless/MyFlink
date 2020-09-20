package com.happiless.flink.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

case class StationLog(sid: String, var callOut: String, var callIn: String, callType: String, callTime: Long, duration: Long)

object CollectionSource {

  def main(args: Array[String]): Unit = {

    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.fromCollection(Array(
      new StationLog("001", "186", "189", "busy", 1577071519462L, 0),
      new StationLog("002", "186", "188", "busy", 1577071520462L, 0),
      new StationLog("003", "183", "188", "busy", 1577071521462L, 0),
      new StationLog("004", "186", "188", "success", 1577071522462L, 32)
    ))
    stream.print()
    streamEnv.execute()
  }

}
