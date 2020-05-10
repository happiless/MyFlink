package com.sxt.flink.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object MyKafkaProducer {

  def main(args: Array[String]): Unit = {
    //连接Kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers","node12:9092,node13:9092,node14:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)

    var producer =new KafkaProducer[String,String](props)
    var r =new Random()
    while(true){ //死循环生成键值对的数据
      val data = new ProducerRecord[String,String]("t_bjsxt","key"+r.nextInt(10),"value"+r.nextInt(100))
      producer.send(data)
      Thread.sleep(1000)
    }
    producer.close()
  }

}
