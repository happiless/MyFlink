package com.sxt.flink

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkStreamingWordCount {

  def main(args: Array[String]): Unit = {

    //1.初始化流式计算的环境
    var streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //3.读取socket中的数据
    var stream: DataStream[String] = streamEnv.socketTextStream("node11", 8888)
    //4.转换和处理数据
    var result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    //5.打印结果
    result.print("result")
    //6.启动流计算程序
    streamEnv.execute("结果")

  }

}
