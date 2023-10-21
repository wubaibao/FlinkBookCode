package com.wubaibao.flinkjava.code.chapter6.partitions;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink自定义分区器实现
 */
public class PartitionCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * Socket中输入数据如下格式数据
         * a,1
         * b,2
         * a,3
         * b,4
         * c,5
         */
        DataStreamSource<String> ds1 = env.socketTextStream("node5", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = ds1.map(one -> {
            String[] arr = one.split(",");
            return Tuple2.of(arr[0], Integer.valueOf(arr[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> result = ds2.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                return key.hashCode() % numPartitions;
            }
        }, new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        result.print();
        env.execute();

    }
}
