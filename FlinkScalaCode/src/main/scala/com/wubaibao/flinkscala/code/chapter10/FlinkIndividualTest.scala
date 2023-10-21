package com.wubaibao.flinkscala.code.chapter10

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util

/**
 * Flink 单独模式匹配测试
 */
object FlinkIndividualTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //1.定义事件流
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //2.定义匹配规则
    val pattern: Pattern[String, String] = Pattern.begin[String]("first").where(_.startsWith("a"))
      .oneOrMore
      .until(_.equals("end"))

    //3.将规则应用到事件流上
    val patternStream: PatternStream[String] = CEP.pattern(ds, pattern).inProcessingTime()

    //4.获取匹配到的数据
    patternStream.select(new PatternSelectFunction[String,String] {
      override def select(pattern: util.Map[String, util.List[String]]): String = {
        val list: util.List[String] = pattern.get("first")
        import scala.collection.JavaConverters._
        list.asScala.mkString("-")
      }
    }).print()

    env.execute()

  }

}
