package com.happiless.flink.state

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestCheckPointByHDFS {
  //使用WordCount案例来测试一下HDFS的状态后端，先运行一段时间Job，然后cansol，在重新启动，看看状态是否是连续的
  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //开启CheckPoint并且设置一些参数
    streamEnv.enableCheckpointing(5000)//每隔5秒开启一次CheckPoint
    streamEnv.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/checkpoint/cp1"))//存放检查点数据

    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.getCheckpointConfig.setCheckpointTimeout(5000)
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//终止job保留检查的数据


    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1
    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //3、读取数据,读取sock流中的数据
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888) //DataStream ==> spark 中Dstream

    //4、转换和处理数据
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1)).setParallelism(2)
      .keyBy(0)//分组算子  : 0 或者 1 代表下标。前面的DataStream[二元组] , 0代表单词 ，1代表单词出现的次数
      .sum(1).setParallelism(2) //聚会累加算子

    //5、打印结果
    result.print("结果").setParallelism(1)
    //6、启动流计算程序
    streamEnv.execute("wordcount")
  }
}
