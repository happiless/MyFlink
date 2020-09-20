package com.happiless.flink.tableAndSql

import com.happiless.flink.source.StationLog
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TestUDFByWordCount {

  //使用tableAPI实现WordCount
  def main(args: Array[String]): Unit = {
    //使用Flink原生的代码创建TableEnvironment
    //先初始化流计算的上下文
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    //两个隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._


    //读取数据源
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888)

    val table: Table = tableEnv.fromDataStream(stream,'line)

    //使用TableAPI切割单词，需要自定义一个切割单词的函数
    val my_func = new MyFlatMapFunction //创建一个UDF
    val result: Table = table.flatMap(my_func('line)).as('word, 'word_c)
      .groupBy('word)
      .select('word, 'word_c.sum as 'c)

    tableEnv.toRetractStream[Row](result).filter(_._1==true).print()

    tableEnv.execute("table_api")

  }

  //自定义UDF
  class MyFlatMapFunction extends TableFunction[Row]{
    //定义函数处理之后的返回类型,输出单词和1
    override def getResultType: TypeInformation[Row] = Types.ROW(Types.STRING(),Types.INT())

    //函数主体
    def eval (str:String):Unit ={
      str.trim.split(" ").foreach(word=>{
        var row =new Row(2)
        row.setField(0,word)
        row.setField(1,1)
        collect(row)
      })
    }
  }
}
