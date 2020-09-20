package com.happiless.flink.window

import com.happiless.flink.source.StationLog
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TestAggregatFunctionByWindow {

  //每隔3秒计算最近5秒内，每个基站的日志数量
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
      //timeWindow
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))//开窗，滑动窗口
      .aggregate(new MyAggregateFunction,new MyWindowFunction)
      .print()

    streamEnv.execute()
  }

  /**
   * 里面的add方法，是来一条数据执行一次，getResult 在窗口结束的时候执行一次
   */
  class MyAggregateFunction extends AggregateFunction[(String,Int),Long,Long]{
    override def createAccumulator(): Long = 0 //初始化一个累加器，开始的时候为0

    override def add(value: (String, Int), accumulator: Long): Long = accumulator+value._2

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  //WindowFunction 输入数据来自于AggregateFunction ，在窗口结束的时候先执行AggregateFunction对象的getResult，然后在执行apply
  class MyWindowFunction extends WindowFunction[Long,(String,Long),String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long)]): Unit = {
      out.collect((key,input.iterator.next())) //next得到第一个值，迭代器中只有一个值
    }
  }
}
