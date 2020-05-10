package com.sxt.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

class MyCustomerSource extends SourceFunction[StationLog] {

  var flag = true

  override def run(sourceContext: SourceFunction.SourceContext[StationLog]): Unit = {
    val r: Random = new Random()
    var types = Array("fail","busy","barring","success")
    while (flag) {
        1.to(5).map(i => {
            var callOut = "1860000%04d".format(r.nextInt(10000))
            var callIn = "1890000%04d".format(r.nextInt(10000))
            new StationLog("station_" + r.nextInt(10), callOut, callIn, types(r.nextInt(4)), System.currentTimeMillis(), r.nextInt(20))
          }
        ).foreach(sourceContext.collect(_))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = flag = false
}

object CustomerSource {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)

    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    stream.print()

    streamEnv.execute()

  }

}
