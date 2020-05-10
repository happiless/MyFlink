package com.sxt.flink.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("node11", 8888)
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setDatabase(1)
      .setHost("node11")
      .setPort(6379)
      .build()
    result.addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription =
        new RedisCommandDescription(RedisCommand.HSET, "t_wc")

      override def getKeyFromData(t: (String, Int)): String = t._1

      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))
    streamEnv.execute()
  }

}
