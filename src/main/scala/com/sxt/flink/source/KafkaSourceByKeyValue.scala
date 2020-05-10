package com.sxt.flink.source

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
//2、导入隐式转换
import org.apache.flink.streaming.api.scala._
object KafkaSourceByKeyValue {

  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1


    //连接Kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers","node12:9092,node13:9092,node14:9092")
    props.setProperty("group.id","flink002")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")

    //设置Kafka数据源
    val stream: DataStream[(String, String)] = streamEnv.addSource(new FlinkKafkaConsumer[(String,String)]("t_bjsxt",new MyKafkaReader,props))

    stream.print()

    streamEnv.execute()
  }

  //自定义一个类，从Kafka中读取键值对的数据
  class MyKafkaReader extends  KafkaDeserializationSchema[(String,String)]{
    //是否流结束
    override def isEndOfStream(nextElement: (String, String)): Boolean = {
      false
    }
    //反序列化
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
      if(record!=null){
        var key="null"
        var value="null"
        if(record.key()!=null){
          key =new String(record.key(),"UTF-8")
        }
        if(record.value()!=null){ //从Kafka记录中得到Value
          value =new String(record.value(),"UTF-8")
        }
        (key,value)
      }else{//数据为空
        ("null","null")
      }
    }

    //指定类型
    override def getProducedType: TypeInformation[(String, String)] ={
      createTuple2TypeInformation(createTypeInformation[String],createTypeInformation[String])
    }
  }
}
