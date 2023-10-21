package com.wubaibao.flinkjava.code.chapter6.partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Reblance 重平衡分区测试
 */
public class ReblanceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("node5", 9999);
        source.rebalance().print("shuffle").setParallelism(3);
        env.execute();
    }

}
