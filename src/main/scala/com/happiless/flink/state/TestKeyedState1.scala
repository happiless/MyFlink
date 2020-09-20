package com.happiless.flink.state

import com.happiless.flink.source.StationLog
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 第一种方法的实现
 * 统计每个手机的呼叫时间间隔，单位是毫秒
 */
object TestKeyedState1 {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    var filePath =getClass.getResource("/station.log").getPath
    val stream: DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })

    stream.keyBy(_.callOut)//分组
      .flatMap(new CallIntervalFunction)
      .print()

    streamEnv.execute()
  }

  //输出的是一个二元组（手机号码，时间间隔）
  class  CallIntervalFunction extends RichFlatMapFunction[StationLog,(String,Long)]{
    //定义一个状态，用于保存前一次呼叫的时间
    private var preCallTimeState:ValueState[Long]=_

    override def open(parameters: Configuration): Unit = {
      preCallTimeState=getRuntimeContext.getState(new ValueStateDescriptor[Long]("pre",classOf[Long]))
    }

    override def flatMap(value: StationLog, out: Collector[(String, Long)]): Unit = {
      //从状态中取得前一次呼叫的时间
      var preCallTime =preCallTimeState.value()
      if(preCallTime==null|| preCallTime==0){ //状态中没有，肯定是第一次呼叫
        preCallTimeState.update(value.callTime)
      }else{ //状态中有数据,则要计算时间间隔
        var interval =Math.abs(value.callTime - preCallTime)
        out.collect((value.callOut,interval))
      }
    }
  }
}
