package com.wubaibao.flinkjava.code.chapter7.checkpoints;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink Checkpoint 状态恢复测试
 */
public class CheckpointRecoverTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，每隔1000ms进行一次checkpoint
        env.enableCheckpointing(1000);

//        //设置状态后端为HashMapStateBackend，状态数据存储在JobManager内存中
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

//        //设置状态后端为HashMapStateBackend，状态数据存储在本地文件系统中
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");

        //设置状态后端为RocksDBStateBackend，状态数据存储在本地文件系统中
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");

//        //设置checkpoint Storage存储为hdfs
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/flink-checkpoints");

        //设置checkpoint清理策略为RETAIN_ON_CANCELLATION
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * Socket输入数据如下：
         * hello,flink
         * hello,checkpoint
         * hello,flink
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
