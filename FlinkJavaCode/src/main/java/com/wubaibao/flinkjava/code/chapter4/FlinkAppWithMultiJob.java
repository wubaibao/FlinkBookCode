package com.wubaibao.flinkjava.code.chapter4;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  一个Flink Application中有多个Flink job 任务
 */
public class FlinkAppWithMultiJob {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取Socket数据 ,获取ds1和ds2
        DataStreamSource<String> ds1 = env.socketTextStream("node5", 8888);
        DataStreamSource<String> ds2 = env.socketTextStream("node5", 9999);

        //3.1 对ds1 直接输出原始数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> transDs1 =
                ds1.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split(",");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        transDs1.print();
        env.executeAsync("first job");

        //3.2 对ds2准备K,V格式数据 ,统计实时WordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS =
                ds2.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split(",");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        tupleDS.keyBy(tp -> tp.f0).sum(1).print();

        //5.execute触发执行
        env.execute("second job");
    }
}
