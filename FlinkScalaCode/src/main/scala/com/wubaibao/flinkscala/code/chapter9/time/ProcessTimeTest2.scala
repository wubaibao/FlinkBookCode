package com.wubaibao.flinkscala.code.chapter9.time

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Flink Table API和SQL编程中指定 Processing Time
 * 案例:读取Socket中基站日志数据，每隔5秒进行数据条数统计
 */
object ProcessTimeTest2 {
  def main(args: Array[String]): Unit = {
    //获取DataStream的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取Taebl API的运行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //将DataStream转换成Table
    val table: Table = tableEnv.fromDataStream(
      ds,
      Schema.newBuilder()
        .columnByExpression("proc_time", "PROCTIME()")
        .build()
    )

    //通过SQL每隔5秒统计一次数据条数
    val resultTbl: Table = tableEnv.sqlQuery("" +
      "select " +
      "TUMBLE_START(proc_time,INTERVAL '5' SECOND) as window_start," +
      "TUMBLE_END(proc_time,INTERVAL '5' SECOND) as window_end," +
      "count(sid) as cnt " +
      "from " + table +
      " group by TUMBLE(proc_time,INTERVAL '5' SECOND)")

    //打印输出
    resultTbl.execute().print()

  }

}
