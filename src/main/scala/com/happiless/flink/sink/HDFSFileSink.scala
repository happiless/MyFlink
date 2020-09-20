package com.happiless.flink.sink

import com.happiless.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object HDFSFileSink {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)

    import org.apache.flink.streaming.api.scala._

    val data: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    val rolling: DefaultRollingPolicy[StationLog, String] = DefaultRollingPolicy.create()
      .withInactivityInterval(2000) //不活动的间隔时间。
      .withRolloverInterval(2000) //每隔两秒生成一个文件 ，重要
      .build()

    val hdfsSink: StreamingFileSink[StationLog] = StreamingFileSink.forRowFormat(
      new Path("hdfs://node11:9000/sink001/"),
      new SimpleStringEncoder[StationLog]("UTF-8")
    ).withBucketCheckInterval(1000) //检查分桶的间隔时间
      .withRollingPolicy(rolling)
      .build()
    data.addSink(hdfsSink)
    streamEnv.execute()

  }

}
