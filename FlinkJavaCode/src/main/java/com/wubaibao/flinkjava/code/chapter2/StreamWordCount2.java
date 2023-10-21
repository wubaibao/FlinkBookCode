package com.wubaibao.flinkjava.code.chapter2;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用 DataStream API Batch 模式来处理WordCount
 */
public class StreamWordCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置批运行模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        //BATCH 设置批处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        //AUTOMATIC 会根据有界流/无界流自动决定采用BATCH/STREAMING模式
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        //STREAMING 设置流处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<String> linesDS = env.readTextFile("./data/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> wordsDS = linesDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String lines, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        wordsDS.keyBy(tp -> tp.f0).sum(1).print();

        env.execute();
    }
}
