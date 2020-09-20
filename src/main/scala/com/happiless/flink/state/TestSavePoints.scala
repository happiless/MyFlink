package com.happiless.flink.state

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestSavePoints {

  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1
    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //3、读取数据,读取sock流中的数据
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888) //DataStream ==> spark 中Dstream
      .uid("socket001")
    //4、转换和处理数据
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .uid("flatmap001")
      .map((_, 1)).setParallelism(2)
      .uid("map001")
      .keyBy(0)//分组算子  : 0 或者 1 代表下标。前面的DataStream[二元组] , 0代表单词 ，1代表单词出现的次数
      .sum(1)
        .uid("sum001")

    //5、打印结果
    result.print("结果").setParallelism(1)
    //6、启动流计算程序
    streamEnv.execute("wordcount")
  }
}
