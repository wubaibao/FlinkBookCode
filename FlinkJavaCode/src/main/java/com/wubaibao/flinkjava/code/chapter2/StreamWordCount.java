package com.wubaibao.flinkjava.code.chapter2;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Java :Flink DataStream 流式处理 WordCount
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //设置Flink运行环境，如果在本地启动则创建本地环境，如果是在集群中启动，则创建集群环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //指定并行度创建本地环境
//        LocalStreamEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment(5);
//        //指定远程JobManagerIp 和RPC 端口以及运行程序所在Jar包及其依赖包
//        StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("JobManagerHost", 6021, 5, "application.jar");

        //2.读取文件数据
        DataStreamSource<String> lines = env.readTextFile("./data/words.txt");

        //3.切分单词，设置KV格式数据
        SingleOutputStreamOperator<Tuple2<String, Long>> kvWordsDS =
                lines.flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4.分组统计获取 WordCount 结果
        kvWordsDS.keyBy(tp->tp.f0).sum(1).print();

        //5.流式计算中需要最后执行execute方法
        env.execute();

    }
}
