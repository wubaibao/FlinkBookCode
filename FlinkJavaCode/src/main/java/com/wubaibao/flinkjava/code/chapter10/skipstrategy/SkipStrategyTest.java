package com.wubaibao.flinkjava.code.chapter10.skipstrategy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * Flink CEP - 循环模式中跳过策略测试
 */
public class SkipStrategyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.定义事件流
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //2.定义匹配规则
        //Pattern<String, String> pattern = Pattern.<String>begin("start")
//        Pattern<String, String> pattern = Pattern.<String>begin("start", AfterMatchSkipStrategy.noSkip())
//        Pattern<String, String> pattern = Pattern.<String>begin("start", AfterMatchSkipStrategy.skipToNext())
//        Pattern<String, String> pattern = Pattern.<String>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
//        Pattern<String, String> pattern = Pattern.<String>begin("start", AfterMatchSkipStrategy.skipToFirst("start"))
        Pattern<String, String> pattern = Pattern.<String>begin("start", AfterMatchSkipStrategy.skipToLast("start"))
                .where(SimpleCondition.of(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.startsWith("a");
                    }
                }))
                .oneOrMore()
                .followedBy("middle").where(SimpleCondition.of(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.startsWith("b");
                    }
                }));

        //3.应用规则
        PatternStream<String> patternStream = CEP.pattern(ds, pattern).inProcessingTime();

        //4.获取匹配到的数据
        patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                String start = pattern.get("start").toString();
                String middle = pattern.get("middle").toString();
                return start+"-"+middle;
            }
        }).print();

        env.execute();
    }
}
