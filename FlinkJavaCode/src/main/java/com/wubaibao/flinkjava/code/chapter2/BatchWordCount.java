package com.wubaibao.flinkjava.code.chapter2;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Java : Flink DataSet 批处理 WordCount
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //设置Flink运行环境，如果在本地启动则创建本地环境，如果是在集群中启动，则创建集群环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //指定并行度创建本地环境
//        LocalEnvironment localEnv = ExecutionEnvironment.createLocalEnvironment(10);
//        //指定远程JobManagerIp 和RPC 端口以及运行程序所在Jar包及其依赖包
//        ExecutionEnvironment romoteEnv = ExecutionEnvironment.createRemoteEnvironment("JobManagerHost", 6021, 5, "application.jar");

        //1.读取文件
        DataSource<String> linesDS = env.readTextFile("./data/words.txt");

        //2.切分单词
        FlatMapOperator<String, String> wordsDS =
                linesDS.flatMap((String lines, Collector<String> collector) -> {
            String[] arr = lines.split(" ");
            for (String word : arr) {
                collector.collect(word);
            }
        }).returns(Types.STRING);

        //3.将单词转换成Tuple2 KV 类型
        MapOperator<String, Tuple2<String, Long>> kvWordsDS =
                wordsDS.map(word -> Tuple2.of(word, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4.按照key 进行分组处理得到最后结果并打印
        kvWordsDS.groupBy(0).sum(1).print();

    }
}
