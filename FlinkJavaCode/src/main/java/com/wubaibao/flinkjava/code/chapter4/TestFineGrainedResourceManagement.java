package com.wubaibao.flinkjava.code.chapter4;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 测试Flink细粒度资源管理
 * Configuration configuration = new Configuration();
 *    configuration.setString("cluster.fine-grained-resource-management.enabled","true");
 *  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
 */
public class TestFineGrainedResourceManagement {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.创建SSG 对象，指定使用资源
        SlotSharingGroup ssg = SlotSharingGroup.newBuilder("ssg")
                .setCpuCores(0.1)
                .setTaskHeapMemoryMB(20)
                .build();

        //3.读取Socket数据
        SingleOutputStreamOperator<String> sourceDS = env.socketTextStream("node5", 9999).slotSharingGroup("ssg");


        //4.对数据进行单词切分
        SingleOutputStreamOperator<String> wordDS = sourceDS.flatMap((String line, Collector<String> collector) -> {
            String[] words = line.split(",");
            for (String word : words) {
                collector.collect(word);
            }
        }).returns(Types.STRING).slotSharingGroup("ssg");

        //5.对单词进行设置PairWord
        SingleOutputStreamOperator<Tuple2<String, Integer>> pairWordDS =
                wordDS.map(s -> new Tuple2<>(s, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).slotSharingGroup("ssg");

        //6.统计单词
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = pairWordDS.keyBy(tp -> tp.f0)
                .sum(1).slotSharingGroup("ssg");

        //7.打印结果
        result.print().slotSharingGroup("ssg");

        //8.注册SSG 对象
        env.registerSlotSharingGroup(ssg);

        //9.execute触发执行
        env.execute();
    }
}
