package com.wubaibao.flinkjava.code.chapter6.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Flink Redis Sink 测试
 */
public class RedisSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Socket中输入数据如下：
         * hello,flink
         * hello,spark
         * hello,hadoop
         * hello,java
         */
        DataStreamSource<String> ds1 = env.socketTextStream("node5", 9999);

        //统计wordcount
        SingleOutputStreamOperator<Tuple2<String, Integer>> result =
                ds1.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
                    String[] arr = s.split(",");
                    for (String word : arr) {
                        collector.collect(word);
                    }
                }).returns(Types.STRING)
                .map(one -> Tuple2.of(one, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tp -> tp.f0)
                .sum(1);

        //准备RedisSink对象
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("node4")
                .setPort(6379)
                .setDatabase(1)
                .build();

        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<>(conf, new RedisMapper<Tuple2<String, Integer>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                //指定Redis命令描述，不需预先创建Redis表
                return new RedisCommandDescription(RedisCommand.HSET, "flink-redis-sink");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> tp) {
                //指定Redis Key
                return tp.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> tp) {
                //指定Redis Value
                return tp.f1 + "";
            }
        });

        //将结果写入Redis
        result.addSink(redisSink);
        env.execute();


    }
}
