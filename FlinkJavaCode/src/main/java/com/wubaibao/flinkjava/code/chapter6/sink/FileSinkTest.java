package com.wubaibao.flinkjava.code.chapter6.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * Flink FileSink 测试
 * 读取socket数据写入到HDFS文件中
 */
public class FileSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启Checkpoint
        env.enableCheckpointing(1000);
        //方便看到结果设置并行度为2
        env.setParallelism(2);

        /**
         * socket 中输入数据如下：
         *  001,186,187,busy,1000,10
         *  002,187,186,fail,2000,20
         *  003,186,188,busy,3000,30
         *  004,188,186,busy,4000,40
         *  005,188,187,busy,5000,50
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //准备FileSink对象
        FileSink<String> fileSink = FileSink.forRowFormat(new Path("./output/java-file-result"),
                        new SimpleStringEncoder<String>("UTF-8"))
                //生成新桶目录的检查周期，默认1分钟
                .withBucketCheckInterval(1000)
                //设置文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                //桶不活跃的间隔时长，默认1分钟
                                .withInactivityInterval(Duration.ofSeconds(30))
                                //设置文件多大后生成新的文件，默认128M
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                //设置每隔多长时间生成一个新的文件，默认1分钟
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .build())
                .build();

        //写出数据到文件
//        ds.sinkTo(fileSink);

        env.execute();
    }
}
