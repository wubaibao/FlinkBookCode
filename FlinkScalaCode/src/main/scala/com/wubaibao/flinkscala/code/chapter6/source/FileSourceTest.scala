package com.wubaibao.flinkscala.code.chapter6.source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FileSourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val fileSource: FileSource[String] = FileSource.forRecordStreamFormat(
      new TextLineInputFormat(),
      new Path("hdfs://mycluster/flinkdata/data.txt")
    ).build()

    val dataStream: DataStream[String] = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source")

    dataStream.print()

    env.execute()
  }

}
