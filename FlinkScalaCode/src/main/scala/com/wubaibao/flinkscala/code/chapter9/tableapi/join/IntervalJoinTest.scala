package com.wubaibao.flinkscala.code.chapter9.tableapi.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
 *
 * Flink Table API - IntervalJoin测试
 * 案例：读取用户登录流和广告点击流，通过IntervalJoin分析用户点击广告的行为。
 *
 */
object IntervalJoinTest {
  def main(args: Array[String]): Unit = {
    //获取DataStream的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取Taebl API的运行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //读取socket-8888中基站用户登录数据，并转换为Tuple类型DataStream
    //user_1,1000
    val loginInfo: DataStream[(String, Long)] = env.socketTextStream("node5", 8888)
      .map(line => {
        val arr = line.split(",")
        (arr(0).trim, arr(1).trim.toLong)
      })

    //读取socket-9999中用户点击广告数据并转换为Tuple类型DataStream
    //user_1,product_1,1000
    val clickInfo: DataStream[(String, String, Long)] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr = line.split(",")
        (arr(0).trim, arr(1).trim, arr(2).trim.toLong)
      })

    val loginTbl: Table = tableEnv.fromDataStream(loginInfo,
      Schema.newBuilder()
        .column("_1", DataTypes.STRING())
        .column("_2", DataTypes.BIGINT())
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_2,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
        .build())
      .as("left_uid", "left_dt", "left_rowtime")

    val clickTbl: Table = tableEnv.fromDataStream(clickInfo,
      Schema.newBuilder()
        .column("_1", DataTypes.STRING())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.BIGINT())
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_3,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
        .build())
      .as("right_uid", "right_adv", "right_dt", "right_rowtime")


    //打印表结构
    loginTbl.printSchema()
    clickTbl.printSchema()

    //interval join
    val result: Table = loginTbl.join(clickTbl)
      .where($"left_uid" === $"right_uid" &&
        $"right_rowtime" >= $"left_rowtime" - 2.second() &&
        $"right_rowtime" < $"left_rowtime" + 2.second()
      ).select($"left_uid", $"left_rowtime", $"right_adv", $"right_rowtime")

    result.execute().print()

  }

}
