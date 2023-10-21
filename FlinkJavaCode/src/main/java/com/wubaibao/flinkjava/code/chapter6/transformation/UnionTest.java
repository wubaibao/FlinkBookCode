package com.wubaibao.flinkjava.code.chapter6.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Flink Union 算子测试
 */
public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> ds2 = env.fromCollection(Arrays.asList(5, 6, 7, 8));
        ds1.union(ds2).print();
        env.execute();
    }
}
