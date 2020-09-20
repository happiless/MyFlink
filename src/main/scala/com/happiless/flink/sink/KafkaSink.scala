package com.happiless.flink.sink

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaSink {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("node11", 8888)
    val result = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //Kafka生产者配置
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers","node12:9092,node13:9092,node14:9092")

    //Kafka写入key，value格式的数据

    result.print()

    val kafkaSink: FlinkKafkaProducer[(String, Int)] = new FlinkKafkaProducer[(String, Int)](
      "t_2020",
      new KafkaSerializationSchema[(String, Int)] {
        override def serialize(t: (String, Int), aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("t_2020", t._1.getBytes, (t._2 + "").getBytes())
        }
      },
      props,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )
    result.addSink(kafkaSink)
    streamEnv.execute()
  }

}
