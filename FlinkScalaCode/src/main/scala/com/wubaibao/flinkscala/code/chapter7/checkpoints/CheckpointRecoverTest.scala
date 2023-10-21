package com.wubaibao.flinkscala.code.chapter7.checkpoints

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * Flink Checkpoint 状态恢复测试
 */
object CheckpointRecoverTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 开启checkpoint，每隔1000ms进行一次checkpoint
    env.enableCheckpointing(1000)

    // 设置checkpoint Storage存储为hdfs
    env.getCheckpointConfig.setCheckpointStorage("hdfs://mycluster/flink-checkpoints")

    // 设置checkpoint清理策略为RETAIN_ON_CANCELLATION
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /**
     * Socket输入数据如下：
     * hello,flink
     * hello,checkpoint
     * hello,flink
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.flatMap(line=>{line.split(",")})
      .map(word=>{(word,1)})
      .keyBy(_._1)
      .sum(1)
      .print()
    env.execute()
  }

}
