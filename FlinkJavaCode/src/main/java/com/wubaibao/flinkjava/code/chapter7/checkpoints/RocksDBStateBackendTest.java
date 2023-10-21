package com.wubaibao.flinkjava.code.chapter7.checkpoints;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink RocksDBStateBackend 状态恢复测试
 * 在Flink 集群中配置flink-conf.yaml 文件实现
 */
public class RocksDBStateBackendTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，每隔1000ms进行一次checkpoint
        env.enableCheckpointing(1000);

        //设置checkpoint清理策略为RETAIN_ON_CANCELLATION
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * Socket输入数据如下：
         * hello,flink
         * hello,flink
         * hello,rocksdb
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        ds.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        }).keyBy(one->one.f0).sum(1).print();

        env.execute();
    }
}
