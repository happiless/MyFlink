package com.happiless.flink.transformation

import com.happiless.flink.source.{MyCustomerSource, StationLog}
import com.happiless.flink.source.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object TestProcessFunction {

  //监控每一个手机号码，如果这个号码在5秒内，所有呼叫它的日志都是失败的，则发出告警信息
  //如果在5秒内只要有一个呼叫不是fail则不用告警
  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("node11", 8888)

    val result = stream.map(line => {
      val arr = line.split(",")
      StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
    })
    result.print()
    result.keyBy(_.callIn)
      .process(new MonitorCallFail)
      .print()
    streamEnv.execute()
  }

  class MonitorCallFail extends KeyedProcessFunction[String, StationLog, String]{

    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

    override def processElement(value: StationLog, context: KeyedProcessFunction[String, StationLog, String]#Context, collector: Collector[String]): Unit = {

      val time: Long = timeState.value()
      //使用一个状态对象记录时间
      if(time == 0 && value.callType.equals("fail")){
        val nowTime: Long = context.timerService().currentProcessingTime()
        val onTime: Long = nowTime + 8 * 1000L //8秒后触发
        context.timerService().registerProcessingTimeTimer(onTime)
        timeState.update(onTime)
      }
      if(time != 0 && !value.callType.equals("fail")){
        context.timerService().deleteProcessingTimeTimer(time)
        timeState.clear()
      }
    }

    //时间到了，执行定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
      var warnStr = s"触发时间:${timestamp}, 手机号: ${ctx.getCurrentKey}"
      out.collect(warnStr)
      timeState.clear()
    }
  }

}
