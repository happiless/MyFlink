package com.happiless.flink.window

import com.happiless.flink.source.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object TestReduceFunctionByWindow {

  //每隔5秒统计每个基站的日志数量
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })

    //开窗
    stream.map(log=>((log.sid,1)))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))//开窗
      .reduce((t1,t2) =>(t1._1,t1._2+t2._2))
      .print()

    streamEnv.execute()
  }
}
