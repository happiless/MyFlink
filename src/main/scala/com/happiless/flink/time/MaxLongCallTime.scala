package com.happiless.flink.time

import com.happiless.flink.source.StationLog
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 每隔5秒统计一下最近10秒内，每个基站中通话时间最长的一次通话发生的时间还有，
 * 主叫号码，被叫号码，通话时长，并且还得告诉我们当前发生的时间范围（10秒）
 */
object MaxLongCallTime {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    //设置时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })
    //引入Watermark(数据是有序)
      .assignAscendingTimestamps(_.callTime) //参数中指定Eventtime具体的值是什么

    //分组、开窗
    stream.filter(_.callType.equals("success")).keyBy(_.sid)
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .reduce(new MyReduceFunction(),new ReturnMaxTimeWindowFunction)
      .print()

    streamEnv.execute()

  }

  class MyReduceFunction() extends ReduceFunction[StationLog]{ //增量聚合
    override def reduce(value1: StationLog, value2: StationLog): StationLog = {
      if(value1.duration>value2.duration) value1 else value2
    }
  }

  class ReturnMaxTimeWindowFunction extends WindowFunction[StationLog,String,String,TimeWindow]{ //在窗口触发的才调用一次
    override def apply(key: String, window: TimeWindow, input: Iterable[StationLog], out: Collector[String]): Unit = {
      var value=input.iterator.next()
      var sb =new StringBuilder
      sb.append("窗口范围是:").append(window.getStart).append("----").append(window.getEnd)
      sb.append("\n")
      sb.append("呼叫时间：").append(value.callTime)
        .append("主叫号码：").append(value.callOut)
        .append("被叫号码：").append(value.callIn)
        .append("通话时长：").append(value.duration)
      out.collect(sb.toString())
    }
  }

}
