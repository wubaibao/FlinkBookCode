package com.wubaibao.flinkscala.code.chapter6.kryoser

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 用户自定义Kryo序列化测试
 *  这里需要使用 Java 创建Student类及对应的序列化类
 */
object KryoSerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    // 注册自定义的Kryo序列化类
    env.getConfig.registerTypeWithKryoSerializer(classOf[Student], classOf[StudentSerializer])

    // 用户基本信息
    env.fromCollection(Seq(
      "1,zs,18",
      "2,ls,20",
      "3,ww,19"
    )).map(one => {
      val split = one.split(",")
      new Student(split(0).toInt, split(1), split(2).toInt)
    }).filter(_.id > 1)
      .print()

    env.execute()
  }

}


