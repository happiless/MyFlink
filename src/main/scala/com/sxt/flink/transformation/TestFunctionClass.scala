package com.sxt.flink.transformation

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import com.sxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestFunctionClass {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val url: URL = getClass.getResource("/station.log")
    val data: DataStream[String] = streamEnv.readTextFile(url.getPath)
    val result: DataStream[StationLog] = data.map(line => {
      val arr: Array[String] = line.split(",")
      StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
    })

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    result.filter(_.callType.equals("success")).map(new MapFunction[StationLog, String] {
      override def map(t: StationLog): String = {
        var strartTime=t.callTime;
        var endTime =t.callTime + t.duration*1000
        "主叫号码:"+t.callOut +",被叫号码:"+t.callIn+",呼叫起始时间:"+format.format(new Date(strartTime))+",呼叫结束时间:"+format.format(new
        Date(endTime))
      }
    }).print()

    streamEnv.execute()
  }

}
