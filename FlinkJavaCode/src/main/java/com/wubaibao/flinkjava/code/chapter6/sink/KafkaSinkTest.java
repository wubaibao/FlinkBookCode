package com.wubaibao.flinkjava.code.chapter6.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink写出数据到Kafka测试
 */
public class KafkaSinkTest {
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
        SingleOutputStreamOperator<String> result = ds1.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
                    String[] arr = s.split(",");
                    for (String word : arr) {
                        collector.collect(word);
                    }
                }).returns(Types.STRING)
                .map(one -> Tuple2.of(one, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tp -> tp.f0)
                .sum(1)
                .map(one -> one.f0 + "-" + one.f1).returns(Types.STRING);

        //准备Flink KafkaSink对象
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                //设置事务超时时间，最大不超过kafka broker的事务最大超时时间限制：max.transaction.timeout.ms
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000L + "")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        //将结果写出到Kafka
        result.sinkTo(kafkaSink);

        env.execute();

    }
}
