package com.wubaibao.flinkjava.code.chapter4;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 测试Flink SlotSharingGroup -SSG
 */
public class TestSSG {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置代码总体并行度为3
        env.setParallelism(6);

        //3.读取Socket数据
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //3.对数据进行过滤
        SingleOutputStreamOperator<String> filterDS = ds.filter(s -> s.startsWith("a"));

        //4.对数据进行单词切分
        SingleOutputStreamOperator<String> wordDS = filterDS.flatMap((String line, Collector<String> collector) -> {
            String[] words = line.split(",");
            for (String word : words) {
                collector.collect(word);
            }
        }).returns(Types.STRING);

        //5.对单词进行设置PairWord
        SingleOutputStreamOperator<Tuple2<String, Integer>> pairWordDS =
                wordDS.map(s -> new Tuple2<>(s, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        //6.统计单词
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = pairWordDS.keyBy(tp -> tp.f0).sum(1).slotSharingGroup("my-ssg-group");

        //7.打印结果
        result.print().slotSharingGroup("default");

        //8.execute触发执行
        env.execute();
    }

}
