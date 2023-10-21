package com.wubaibao.flinkscala.code.chapter9.dsandtableintegration.dstotable

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.{Row, RowKind}


/**
 * Flink DataStream 转换为 Table
 * 使用 tableEnv.fromChangelogStream() 方法将 DataStream 转换为 Table
 */
object FromChangelogStreamTest {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //创建TableEnv
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    val dataStream:
      DataStream[Row] = env.fromElements(
      Row.ofKind(RowKind.INSERT, "zs", Int.box(18)),
      Row.ofKind(RowKind.INSERT, "ls", Int.box(19)),
      Row.ofKind(RowKind.UPDATE_BEFORE, "zs", Int.box(18)),
      Row.ofKind(RowKind.UPDATE_AFTER, "zs", Int.box(20)),
      Row.ofKind(RowKind.DELETE, "ls", Int.box(19))
    )(Types.ROW(Types.STRING, Types.INT))

    //将DataStream 转换成 Table
    val result: Table = tableEnv.fromChangelogStream(
      dataStream,
      //通过Schema指定主键
      Schema.newBuilder.primaryKey("f0").build,
      //指定ChangelogMode，这里使用all()，表示所有类型的数据都会被处理
      ChangelogMode.all()
    )

    //打印表结构
    result.printSchema()

    //打印表数据
    result.execute.print()

  }

}
