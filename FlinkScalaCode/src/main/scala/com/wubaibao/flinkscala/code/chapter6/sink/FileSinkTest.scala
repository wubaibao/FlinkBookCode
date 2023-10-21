package com.wubaibao.flinkscala.code.chapter6.sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.time.Duration

/**
 * Flink 写出到文件测试
 */
object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    //设置并行度为2，方便测试
    env.setParallelism(2)

    /**
     * Socket中输入数据如下：
     *  001,186,187,busy,1000,10
     *  002,187,186,fail,2000,20
     *  003,186,188,busy,3000,30
     *  004,188,186,busy,4000,40
     *  005,188,187,busy,5000,50
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //设置FlinkSink
    val fileSink: FileSink[String] = FileSink.forRowFormat(new Path("./out/scala-file-out"),
      new SimpleStringEncoder[String]("UTF-8"))
      //设置桶目录检查间隔，默认1分钟
      .withBucketCheckInterval(1000 * 60)
      //设置滚动策略
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          //桶不活跃的间隔时长，默认1分钟
          .withInactivityInterval(Duration.ofSeconds(30))
          //设置文件多大后生成新的文件，默认128M
          .withMaxPartSize(MemorySize.ofMebiBytes(1024))
          //设置每隔多长时间生成新的文件，默认1分钟
          .withRolloverInterval(Duration.ofSeconds(10))
          .build()
      )
      .build()

    //写出到文件
    ds.sinkTo(fileSink)

    env.execute()

  }

}
