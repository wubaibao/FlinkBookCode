package com.wubaibao.flinkjava.code.chapter7.savepoints;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink SavePoint 状态恢复测试
 */
public class SavePointTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取socket数据做wordcount
        SingleOutputStreamOperator<String> lines = env.socketTextStream("node5", 9999).uid("socket-source");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2 =
                lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).uid("flatmap");

        SingleOutputStreamOperator<Tuple2<String, Integer>> result =
                tuple2.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1).uid("sum");

        result.print().uid("print");

        env.execute();

    }
}
