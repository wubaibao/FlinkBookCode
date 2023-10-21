package com.wubaibao.flinkjava.code.chapter10.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP - 恶意登录检测
 * 案例：读取用户登录日志，如果一个用户在20秒内连续三次登录失败，则是恶意登录，输出告警信息
 */
public class LoginDetectTest {
    public static void main(String[] args) throws Exception {
        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.定义事件流
        SingleOutputStreamOperator<LoginInfo> ds = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, LoginInfo>() {
                    @Override
                    public LoginInfo map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new LoginInfo(arr[0], arr[1], Long.valueOf(arr[2]), arr[3]);
                    }
                });

        //设置watermark并设置自动推进watermark
        SingleOutputStreamOperator<LoginInfo> dsWithWatermark = ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginInfo>() {
                            @Override
                            public long extractTimestamp(LoginInfo element, long recordTimestamp) {
                                return element.getLoginTime();
                            }
                        }).withIdleness(Duration.ofSeconds(5))
        );

        //获取每个用户的登录信息
        KeyedStream<LoginInfo, String> keyedStream = dsWithWatermark.keyBy(new KeySelector<LoginInfo, String>() {
            @Override
            public String getKey(LoginInfo value) throws Exception {
                return value.getUid();
            }
        });

        //2.定义匹配规则。
        Pattern<LoginInfo, LoginInfo> pattern = Pattern.<LoginInfo>begin("first")
                .where(new SimpleCondition<LoginInfo>() {
                    @Override
                    public boolean filter(LoginInfo value) throws Exception {
                        return value.getLoginState().startsWith("fail");
                    }
                }).times(3).within(Time.seconds(20));

        //3.将匹配规则应用到数据流上
        PatternStream<LoginInfo> patternStream = CEP.pattern(keyedStream, pattern);

        //4.获取符合规则的数据
        patternStream.process(new PatternProcessFunction<LoginInfo, String>() {
            @Override
            public void processMatch(Map<String, List<LoginInfo>> match,
                                     Context ctx,
                                     Collector<String> out) throws Exception {
                List<LoginInfo> firstPatternInfo = match.get("first");

                //获取用户
                String uid = firstPatternInfo.get(0).getUid();

                //输出
                out.collect("用户：" + uid + "在20秒内连续3次登录失败!");
            }
        }).print();

        env.execute();
    }
}
