package com.wubaibao.flinkscala.code.chapter7.checkpoints

import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink RocksDBStateBackend 状态恢复测试
 * 在Flink 集群中配置flink-conf.yaml 文件实现
 */
object RocksDBStateBackendTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 开启checkpoint，每隔1000ms进行一次checkpoint
    env.enableCheckpointing(1000)

    // 设置checkpoint清理策略为RETAIN_ON_CANCELLATION
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /**
     * Socket输入数据如下：
     * hello,flink
     * hello,flink
     * hello,rocksdb
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    ds.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }

}
