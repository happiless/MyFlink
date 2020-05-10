package com.sxt.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sxt.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CustomerJDBCSink {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)

    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    stream.addSink(new MyCustomerSink)

    streamEnv.execute("jdbcSink")

  }

  class MyCustomerSink extends RichSinkFunction[StationLog](){

    var conn : Connection = _
    var pstm : PreparedStatement = _

    override def invoke(value: StationLog, context: SinkFunction.Context[_]): Unit = {
      pstm.setString(1, value.sid)
      pstm.setString(2, value.callIn)
      pstm.setString(3, value.callOut)
      pstm.setString(4, value.callType)
      pstm.setLong(5, value.callTime)
      pstm.setLong(6, value.duration)

      pstm.executeUpdate()
    }

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://node11/test","root","123")
      pstm = conn.prepareStatement("insert into t_station_log (sid,call_in,call_out,call_type,call_time,duration) values (?,?,?,?,?,?)")
    }

    override def close(): Unit = {
      pstm.close()
      conn.close()
    }
  }

}
