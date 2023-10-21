package com.wubaibao.flinkscala.code.chapter6.sink

import com.wubaibao.flinkscala.code.chapter6.StationLog
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource
import org.apache.flink.connector.jdbc.{JdbcExactlyOnceOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.function.SerializableSupplier

import java.sql.PreparedStatement
import javax.sql.XADataSource

/**
 * Flink JdbcSink ExactlyOnce测试
 */
object JdbcSinkExactlyOnceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //必须设置checkpoint，否则数据不能写入mysql
    env.enableCheckpointing(5000)

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

    //准备Flink JdbcSink ExactlyOnce方式
    // 注意：这里的JdbcSink不能使用JdbcSink.sink，而是使用JdbcSink.exactlyOnceSink
    val JdbcExactlyOnceSink: SinkFunction[StationLog] = JdbcSink.exactlyOnceSink(
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
      JdbcExecutionOptions.builder
        //批次提交大小，默认500
        .withBatchSize(1000)
        //批次提交间隔间隔时间，默认0，即批次大小满足后提交
        .withBatchIntervalMs(1000)
        //最大重试次数，默认3,JDBC XA接收器要求maxRetries等于0，否则可能导致重复。
        .withMaxRetries(0)
        .build(),
      JdbcExactlyOnceOptions.builder
        //只允许每个连接有一个 XA 事务
        .withTransactionPerConnection(true)
        .build(),
      //该方法必须new 方式，否则会报错The implementation of the XaFacade is not serializable. The object probably contains or references non serializable fields.
      new SerializableSupplier[XADataSource] {
        override def get(): XADataSource = {
          val xaDataSource = new MysqlXADataSource
          xaDataSource.setUrl("jdbc:mysql://node2:3306/mydb?useSSL=false")
          xaDataSource.setUser("root")
          xaDataSource.setPassword("123456")
          xaDataSource
        }
      }
    )

    //将数据写入到JdbcSink
    ds.addSink(JdbcExactlyOnceSink)
    env.execute()


  }

}
