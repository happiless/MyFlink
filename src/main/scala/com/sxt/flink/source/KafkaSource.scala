package com.sxt.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaSource {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","fink01")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")
    //设置kafka为数据源
    val stream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("t_topic", new SimpleStringSchema(), props))
    stream.print()
    streamEnv.execute()
  }
}
