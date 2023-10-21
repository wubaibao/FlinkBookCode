package com.wubaibao.flinkscala.code.chapter6.sink

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.sql.PreparedStatement

/**
 * Flink JdbcSink 测试
 */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * Socket中输入数据如下：
     * 001,186,187,busy,1000,10
     * 002,187,186,fail,2000,20
     * 003,186,188,busy,3000,30
     * 004,188,186,busy,4000,40
     * 005,188,187,busy,5000,50
     */
    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //准备Flink JdbcSink
    val jdbcSink: SinkFunction[StationLog] = JdbcSink.sink(
      "insert into station_log(sid,call_out,call_in,call_type,call_time,duration) values(?,?,?,?,?,?)",
      //这里不能使用箭头函数，否则会报：The implementation of the RichOutputFormat is not serializable. The object probably contains or references non serializable fields.
      new JdbcStatementBuilder[StationLog] {
        override def accept(pst: PreparedStatement, stationLog: StationLog): Unit = {
          pst.setString(1, stationLog.sid)
          pst.setString(2, stationLog.callOut)
          pst.setString(3, stationLog.callIn)
          pst.setString(4, stationLog.callType)
          pst.setLong(5, stationLog.callTime)
          pst.setLong(6, stationLog.duration)
        }
      },
      JdbcExecutionOptions.builder()
        //设置批次大小，默认5000
        .withBatchSize(1000)
        //批次提交间隔间隔时间，默认0，即批次大小满足后提交
        .withBatchIntervalMs(200)
        //设置最大重试次数，默认3
        .withMaxRetries(5)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://node2:3306/mydb?useSSL=false")
        .withDriverName("com.mysql.jdbc.Driver")
        .withUsername("root")
        .withPassword("123456")
        .build()
    )

    //数据写出到MySQL
    ds.addSink(jdbcSink)

    env.execute()
  }

}
