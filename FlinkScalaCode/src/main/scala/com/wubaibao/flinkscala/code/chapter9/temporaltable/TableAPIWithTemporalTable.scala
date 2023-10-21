package com.wubaibao.flinkscala.code.chapter9.temporaltable

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.{$, call}
import org.apache.flink.table.api.{DataTypes, Schema, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TemporalTableFunction

object TableAPIWithTemporalTable {
  def main(args: Array[String]): Unit = {
    //获取DataStream的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取Taebl API的运行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //当某个并行度5秒没有数据输入时，自动推进watermark
    tableEnv.getConfig().set("table.exec.source.idle-timeout","5000")

    //读取socket中数据 ,p_001,1000
    val leftInfo: DataStream[(String, Long)] = env.socketTextStream("node5", 8888)
      .map(line => {
        val arr = line.split(",")
        (arr(0), arr(1).toLong)
      })

    val leftTbl: Table = tableEnv.fromDataStream(leftInfo,
      Schema.newBuilder()
        .column("_1", DataTypes.STRING())
        .column("_2", DataTypes.BIGINT())
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_2,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '5' SECOND")
        .build())
      .as("left_product_id", "left_dt", "left_rowtime")

    //读取socket中数据 ,p_001,1000
    val rightInfo: DataStream[(Long,String, String,Double)] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr = line.split(",")
        (arr(0).toLong, arr(1),arr(2),arr(3).toDouble)
      })

    val rightTbl: Table = tableEnv.fromDataStream(rightInfo,
      Schema.newBuilder()
        .column("_1", DataTypes.BIGINT())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.STRING())
        .column("_4", DataTypes.DOUBLE())
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_1,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '5' SECOND")
        .build())
      .as("right_update_time", "right_product_id", "right_product_name", "right_price", "right_rowtime")

    //创建时态表函数，"right_rowtime"为时间属性，"right_product_id"为主键
    val temporalTableFunction: TemporalTableFunction = rightTbl.createTemporalTableFunction($("right_rowtime"), $("right_product_id"))
    tableEnv.createTemporarySystemFunction("temporalTableFunction", temporalTableFunction)

    //使用时态表
    val result: Table = leftTbl.joinLateral(
      call("temporalTableFunction", $("left_rowtime")),
      $("left_product_id").isEqual($("right_product_id"))
    ).select(
      $("left_product_id"),
      $("left_dt"),
      $("right_product_name"),
      $("right_price")
    )

    result.execute.print()

  }

}
