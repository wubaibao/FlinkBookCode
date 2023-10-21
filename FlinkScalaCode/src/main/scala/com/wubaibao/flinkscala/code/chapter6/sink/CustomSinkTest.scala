package com.wubaibao.flinkscala.code.chapter6.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}

object CustomSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * socket 中输入数据如下：
     * 001,186,187,busy,1000,10
     * 002,187,186,fail,2000,20
     * 003,186,188,busy,3000,30
     * 004,188,186,busy,4000,40
     * 005,188,187,busy,5000,50
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    ds.addSink(new RichSinkFunction[String] {
      var conn: Connection = _

      // open方法在sink的生命周期内只会执行一次
      override def open(parameters: Configuration): Unit = {
        val conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "node3,node4,node5")
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        //创建连接
        conn = ConnectionFactory.createConnection(conf)
      }

      // invoke方法在sink的生命周期内会执行多次，每条数据都会执行一次
      override def invoke(currentOne: String, context: SinkFunction.Context): Unit = {
        //解析数据：001,186,187,busy,1000,10
        val split: Array[String] = currentOne.split(",")
        //准备rowkey
        val rowkey = split(0)
        //获取列
        val callOut  = split(1)
        val callIn  = split(2)
        val callType  = split(3)
        val callTime  = split(4)
        val duration  = split(5)

        //获取表对象
        val table = conn.getTable(org.apache.hadoop.hbase.TableName.valueOf("flink-sink-hbase"))
        //准备put对象
        val put = new Put(rowkey.getBytes())
        //添加列
        put.addColumn("cf".getBytes(), "callOut".getBytes(), callOut.getBytes())
        put.addColumn("cf".getBytes(), "callIn".getBytes(), callIn.getBytes())
        put.addColumn("cf".getBytes(), "callType".getBytes(), callType.getBytes())
        put.addColumn("cf".getBytes(), "callTime".getBytes(), callTime.getBytes())
        put.addColumn("cf".getBytes(), "duration".getBytes(), duration.getBytes())
        //插入数据
        table.put(put)
        //关闭表
        table.close()
      }

      // close方法在sink的生命周期内只会执行一次
      override def close(): Unit = super.close()
    })

    env.execute()
  }

}
