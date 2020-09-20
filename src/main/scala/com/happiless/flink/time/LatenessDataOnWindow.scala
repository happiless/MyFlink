package com.happiless.flink.time

import com.happiless.flink.source.StationLog
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LatenessDataOnWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    //设置时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101", 8888)
      .map(line => {
        var arr = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

      //引入Watermark，数据是乱序的，并且大多数数据延迟2秒
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(2)) { //水位线延迟2秒
        override def extractTimestamp(element: StationLog) = {
          element.callTime
        }
      })

    //定义一个侧输出流的标签
    var lateTag =new OutputTag[StationLog]("late")

    //分组，开窗
    val result: DataStream[String] = stream.keyBy(_.sid)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      //设置迟到的数据超出了2秒的情况下，怎么办。交给AllowedLateness处理
      //也分两种情况，第一种：允许数据迟到5秒（迟到2-5秒），再次延迟触发窗口函数。触发的条是：Watermark < end-of-window + allowedlateness
      //第二种：迟到的数据在5秒以上，输出到则流中
      .allowedLateness(Time.seconds(5)) //运行数据迟到5秒，还可以触发窗口
      .sideOutputLateData(lateTag)
      .aggregate(new MyAggregateCountFunction, new OutputResultWindowFunction)

    result.getSideOutput(lateTag).print("late") //迟到时间超过5秒的数据，本来需要另外处理的。
    result.print("main")

    streamEnv.execute()


  }

  //增量聚会的函数
  class MyAggregateCountFunction extends AggregateFunction[StationLog,Long,Long]{
    override def createAccumulator(): Long = 0

    override def add(value: StationLog, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  class OutputResultWindowFunction extends WindowFunction[Long,String,String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
      var value =input.iterator.next()
      var sb =new StringBuilder
      sb.append("窗口的范围:").append(window.getStart).append("---").append(window.getEnd)
      sb.append("\n")
      sb.append("当前的基站ID是:").append(key)
        .append("， 呼叫的数量是:").append(value)
      out.collect(sb.toString())
    }
  }
}
