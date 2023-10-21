package com.wubaibao.flinkjava.code.chapter10.individualpatterns;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * Flink 单独模式匹配测试
 */
public class FlinkIndividualTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.定义事件流
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //2.定义匹配规则
        Pattern<String, String> pattern = Pattern.<String>begin("first")
                .where(SimpleCondition.of(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("a");
            }
        }))
                .oneOrMore()
                .until(new IterativeCondition<String>() {
            @Override
            public boolean filter(String value, Context<String> ctx) throws Exception {
                return value.equals("end");
            }
        });

        //3.应用规则
        PatternStream<String> patternStream = CEP.pattern(ds, pattern).inProcessingTime();

        //4.获取匹配到的数据
        patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                List<String> first = pattern.get("first");
                StringBuilder sb = new StringBuilder();
                for (String s : first) {
                    sb.append(s+"-");
                }
                return sb.substring(0,sb.length()-1);
            }
        }).print();

        env.execute();

    }
}
