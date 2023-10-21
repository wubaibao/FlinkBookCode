package com.wubaibao.flinkjava.code.chapter6.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 读取文件中的数据
 */
public class FileSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Flink1.14版本之前读取文件写法
         */
//        DataStreamSource<String> dataStream = env.readTextFile("hdfs://mycluster/flinkdata/data.txt");
//        dataStream.print();

        /**
         * Flink 读取文件最新写法
         */
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("hdfs://mycluster/flinkdata/data.txt")).build();

        DataStreamSource<String> dataStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");

        dataStream.print();

        env.execute();


    }
}
