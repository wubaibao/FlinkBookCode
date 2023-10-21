package com.wubaibao.flinkjava.code.chapter10.combiningpatterns;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Flink CEP - NotFollowedBy+within 模式匹配策略测试
 */
public class NotFollowedByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.定义事件流
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //2.定义匹配规则
        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.startsWith("a");
                    }
                }))
                .notFollowedBy("middle").where(SimpleCondition.of(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.startsWith("c");
                    }
                }))
                .within(Time.seconds(10));

        //3.应用规则
        PatternStream<String> patternStream = CEP.pattern(ds, pattern).inProcessingTime();

        //4.获取匹配到的数据
        patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                String start = pattern.get("start").get(0);
                return start;
            }
        }).print();

        env.execute();
    }
}
