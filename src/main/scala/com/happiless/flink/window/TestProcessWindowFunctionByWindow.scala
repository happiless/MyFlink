package com.happiless.flink.window

import com.happiless.flink.source.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TestProcessWindowFunctionByWindow {

  //每隔5秒统计每个基站的日志数量
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })

    //开窗
    stream.map(log=>((log.sid,1)))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(5))//开窗
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new ProcessWindowFunction[(String,Int),(String,Long),String,TimeWindow]{ //一个窗口结束的时候调用一次(一个分组执行一次)
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Long)]): Unit = {
          println("------------")
          //注意：整个窗口的数据保存到Iterable，里面有很多行数据。Iterable的size就是日志的总条数
          out.collect((key,elements.size))
        }
      })
      .print()

    streamEnv.execute()
  }
}
