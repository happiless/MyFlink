package com.happiless.flink.tableAndSql

import com.happiless.flink.source.StationLog
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object TestWindowByTableAPI {

  //每隔5秒钟统计，每个基站的通话数量,假设数据是乱序。最多延迟3秒,需要水位线
  def main(args: Array[String]): Unit = {
    //使用Flink原生的代码创建TableEnvironment
    //先初始化流计算的上下文
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义采用EventTime作为时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    //两个隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._



    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })
      //引入Watermark，让窗口延迟触发
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(3)) {
        override def extractTimestamp(element: StationLog) = {
          element.callTime
        }
      })


    //从DataStream中创建动态的Table，并且可以指定EventTime是哪个字段
    var table:Table =tableEnv.fromDataStream(stream,'sid,'callOut,'callIn,'callType,'callTime.rowtime)

    //开窗,滚动窗口,第一种写法
//    table.window(Tumble.over("5.second").on("callTime").as("window"))
    //第二种写法
    val result: Table = table.window(Tumble over 5.second on 'callTime as 'window)
      .groupBy('window, 'sid) //必须使用两个字段分组，分别是窗口和基站ID
      .select('sid, 'window.start, 'window.end, 'sid.count) //聚会计算

    //打印结果
    tableEnv.toRetractStream[Row](result)
      .filter(_._1==true)
      .print()

    tableEnv.execute("sql")

    //如果是滑动窗口
//    table.window(Slide over 10.second every 5.second on 'callTime as 'window)
//    table.window(Slide.over("10.second").every("5.second").on("callTime").as("window"))
  }
}
