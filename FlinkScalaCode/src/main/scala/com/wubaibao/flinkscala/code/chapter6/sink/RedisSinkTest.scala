package com.wubaibao.flinkscala.code.chapter6.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * Flink Redis Sink 测试
 */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置隐式转换
    import org.apache.flink.api.scala._

    /**
     * Socket 中输入数据格式：
     * hello,world
     * hello,flink
     * hello,scala
     * hello,spark
     * hello,hadoop
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //统计wordCount
    val result: DataStream[(String, Int)] = ds.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    //准备RedisSink对象
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("node4")
      .setPort(6379)
      .setDatabase(1)
      .build()
    val redisSink = new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {
      //指定写入Redis的命令
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "flink-scala-redis")
      }

      //指定写入Redis的Key
      override def getKeyFromData(t: (String, Int)): String = t._1

      //指定写入Redis的Value
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    })

    //将结果写入Redis
    result.addSink(redisSink)
    env.execute("RedisSinkTest")



  }

}
