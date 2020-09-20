package com.happiless.flink.transformation

import com.happiless.flink.source.StationLog
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object TestSideOutputStream {

  //把呼叫成功的输出到主流，不成功的输出到测流
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val notSuccessTag: OutputTag[StationLog] = new OutputTag[StationLog]("not_success")
    val data: DataStream[String] = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
    val result: DataStream[StationLog] = data.map(line => {
      val arr = line.split(",")
      StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
    })
    val mainStream: DataStream[StationLog] = result.process(new CreateSideOutputStream(notSuccessTag))
    val sideStream = mainStream.getSideOutput(notSuccessTag)
    mainStream.print("主流")
    sideStream.print("测流")
    streamEnv.execute()
  }

  class CreateSideOutputStream(outputTag: OutputTag[StationLog]) extends ProcessFunction[StationLog, StationLog]{
    override def processElement(value: StationLog, context: ProcessFunction[StationLog, StationLog]#Context, out: Collector[StationLog]): Unit = {
      if(value.callType.equals("success")){
        out.collect(value)
      } else {
        context.output(outputTag, value)
      }
    }
  }

}
