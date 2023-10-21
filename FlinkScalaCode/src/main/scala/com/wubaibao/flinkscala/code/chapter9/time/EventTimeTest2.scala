package com.wubaibao.flinkscala.code.chapter9.time

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Flink Table API和SQL编程中指定 Event Time
 * 案例:读取Kafka中基站日志数据，每隔5秒进行数据条数统计
 */
object EventTimeTest2 {
  def main(args: Array[String]): Unit = {
    //获取DataStream的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取Taebl API的运行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //当某个并行度5秒没有数据输入时，自动推进watermark
    tableEnv.getConfig().set("table.exec.source.idle-timeout","5000")

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    val table: Table = tableEnv.fromDataStream(ds,
      Schema.newBuilder()
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
        .build())

    //通过SQL每隔5秒统计一次数据条数
    val resultTbl: Table = tableEnv.sqlQuery("" +
      "select " +
      "TUMBLE_START(rowtime,INTERVAL '5' SECOND) as window_start," +
      "TUMBLE_END(rowtime,INTERVAL '5' SECOND) as window_end," +
      "count(sid) as cnt " +
      "from " + table +
      " group by TUMBLE(rowtime,INTERVAL '5' SECOND)")

    //打印结果
    resultTbl.execute().print()

  }

}
