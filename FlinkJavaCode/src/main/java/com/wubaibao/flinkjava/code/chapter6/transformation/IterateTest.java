package com.wubaibao.flinkjava.code.chapter6.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Iterate 迭代操作
 */
public class IterateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("node5", 9999);

        //对数据流进行转换
        SingleOutputStreamOperator<Integer> ds2 = ds1.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.valueOf(s);
            }
        });

        //使用 iterate() 方法创建一个迭代流 iterate，用于支持迭代计算
        IterativeStream<Integer> iterate = ds2.iterate();

        //定义迭代体：在迭代流 iterate 上进行映射转换，将每个整数元素减去 1，并返回一个新的数据流
        SingleOutputStreamOperator<Integer> minusOne = iterate.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("迭代体中输入的数据为：" + value);
                return value - 1;
            }
        });

        //定义迭代条件，满足迭代条件的继续进入迭代体进行迭代，否则不迭代
        SingleOutputStreamOperator<Integer> stillGreaterThanZero = minusOne.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 0;
            }
        });

        //对迭代流应用迭代条件
        iterate.closeWith(stillGreaterThanZero);

        //迭代流数据输出，无论是否满足迭代条件都会输出
        iterate.print();
        env.execute();
    }
}
