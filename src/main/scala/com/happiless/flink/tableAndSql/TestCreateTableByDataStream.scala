package com.happiless.flink.tableAndSql

import com.happiless.flink.source.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

object TestCreateTableByDataStream {

  def main(args: Array[String]): Unit = {
    //使用Flink原生的代码创建TableEnvironment
    //先初始化流计算的上下文
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    //两个隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    //读取数据
    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })


    //注册一张表,方法没有返回值
//    tableEnv.registerDataStream("t_table2",stream)

//    tableEnv.sqlQuery("select * from t_table2").printSchema()
    //可以使用SQL API
    //打印表结构,或者使用Table API。需要得到Table对象
//    val table: Table = tableEnv.scan("t_table2")
//    table.printSchema() //打印表结构

    //修改字段名字
//    val table: Table = tableEnv.fromDataStream(stream,'id,'call_out,'call_in) //把后面的字段补全
//    table.printSchema()

    //查询过滤
    val table: Table = tableEnv.fromDataStream(stream)

    //fitler过滤
//    var result:Table=table.filter('callType==="success")
//
//    val ds: DataStream[Row] = tableEnv.toAppendStream[Row](result)
//    ds.print()

    //分组聚和
    val result: Table = table.groupBy('sid).select('sid,'sid.count as 'log_count)
    tableEnv.toRetractStream[Row](result).filter(_._1==true).print() //状态中新增的数据加了一个false标签
    streamEnv.execute()
  }
}
