package com.wubaibao.flinkscala.code.chapter9.state

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Flink Table API中状态测试
 * 案例：读取socket中基站日志数据，按照基站进行分组统计基站的通话时长
 */
object TableAPIStateTest {
  def main(args: Array[String]): Unit = {
    //获取DataStream的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取Taebl API的运行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //设置状态保存时间，默认为0，表示不清理状态，这里设置5s
    tableEnv.getConfig().set("table.exec.state.ttl", "5000")

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //将DataStream转换成Table
    tableEnv.createTemporaryView("station_log", ds)

    //通过SQL统计通话时长信息
    val result = tableEnv.sqlQuery("select sid, sum(duration) as duration from station_log group by sid")

    //打印输出
    result.execute().print()

  }

}
