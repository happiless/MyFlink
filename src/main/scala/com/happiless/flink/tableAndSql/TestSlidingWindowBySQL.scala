package com.happiless.flink.tableAndSql

import com.happiless.flink.source.StationLog
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

object TestSlidingWindowBySQL {

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

    //滑动窗口，窗口大小是10秒，滑动步长是5秒，需求：每隔5秒，统计10秒内，每个基站的成功通话时长总和
    //注册一张表，并且指定EventTime是哪个字段
    tableEnv.registerDataStream("t_station_log",stream,'sid,'callOut,'callIn,'callType,'callTime.rowtime,'duration)


    val result: Table = tableEnv.sqlQuery("select sid, hop_start(callTime,interval '5' second,interval '10' second),hop_end(callTime,interval '5' second,interval '10' second)" +
      ",sum(duration) " +
      "from t_station_log " +
      "where callType='success' " +
      "group by hop(callTime,interval '5' second,interval '10' second),sid")


    //打印结果
    tableEnv.toRetractStream[Row](result)
      .filter(_._1==true)
      .print()

    tableEnv.execute("sql")


  }
}
