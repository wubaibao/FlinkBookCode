package com.wubaibao.flinkscala.code.chapter9.tableapi.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
 * Flink Table API - fullOuterJoin测试
 * 案例：读取socket中数据形成两个流，进行fullOuterJoin操作
 */
object FullOuterJoinTest {
  def main(args: Array[String]): Unit = {
    //获取DataStream的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取Taebl API的运行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //读取socket-8888中基站日志数据并转换为Tuple类型DataStream
    //1,zs,18,1000
    val personInfo: DataStream[(Int, String, Int, Long)] = env.socketTextStream("node5", 8888)
      .map(line => {
        val arr = line.split(",")
        (arr(0).trim.toInt, arr(1).trim, arr(2).trim.toInt, arr(3).trim.toLong)
      })

    //读取socket-9999中基站日志数据并转换为Tuple类型DataStream
    //1,zs,beijing,1000
    val addressInfo: DataStream[(Int, String, String, Long)] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr = line.split(",")
        (arr(0).trim.toInt, arr(1).trim, arr(2).trim, arr(3).trim.toLong)
      })

    val personTbl: Table = tableEnv.fromDataStream(personInfo,
      Schema.newBuilder()
        .column("_1", DataTypes.INT())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.INT())
        .column("_4", DataTypes.BIGINT())
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_4,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
        .build())
      .as("left_id", "left_name", "age", "left_rowtime", "left_ltz_time")

    val addressTbl: Table = tableEnv.fromDataStream(addressInfo,
      Schema.newBuilder()
        .column("_1", DataTypes.INT())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.STRING())
        .column("_4", DataTypes.BIGINT())
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_4,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
        .build())
      .as("right_id", "right_name", "address", "right_rowtime", "right_ltz_time")

    //打印表结构
    personTbl.printSchema()
    addressTbl.printSchema()

    //fullOuterJoin
    val resultTbl: Table = personTbl.fullOuterJoin(addressTbl, $"left_id" === $"right_id" && $"left_name" === $"right_name")
      .select(
        $("left_id"),
        $("left_name"),
        $("age"),
        $("address"),
        $("left_rowtime"),
        $("right_rowtime"))

    resultTbl.execute().print()

  }

}
