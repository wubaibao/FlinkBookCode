package com.wubaibao.flinkjava.code.chapter6.transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink Connect 算子测试
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> ds1 = env.fromCollection(Arrays.asList(Tuple2.of("a", 1), Tuple2.of("b", 2), Tuple2.of("c", 3)));
        DataStreamSource<String> ds2 = env.fromCollection(Arrays.asList("aa","bb","cc"));

        // Connect 两个流，类型可以不一样，只能连接两个流
        ConnectedStreams<Tuple2<String, Integer>, String> connect = ds1.connect(ds2);

        //可以对连接后的流使用map、flatMap、process等算子进行操作，但内部方法使用的是CoMap、CoFlatMap、CoProcess等函数进行处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = connect.process(new CoProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public void processElement1(Tuple2<String, Integer> tuple2, CoProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(tuple2);
            }

            @Override
            public void processElement2(String value, CoProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value, 1));
            }
        });
        result.print();
        env.execute();
    }
}
