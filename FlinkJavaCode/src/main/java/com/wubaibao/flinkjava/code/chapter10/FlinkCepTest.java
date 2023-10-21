package com.wubaibao.flinkjava.code.chapter10;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
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
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP 测试
 * 案例：读取Socket基站日志数据，当同一个基站通话有3次失败时，输出报警信息
 */
public class FlinkCepTest {
    public static void main(String[] args) throws Exception {

        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.定义事件流
        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                    }
                });

        //设置watermark并设置自动推进watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<StationLog>() {
                            @Override
                            public long extractTimestamp(StationLog element, long recordTimestamp) {
                                return element.callTime;
                            }
                        })
                        .withIdleness(Duration.ofSeconds(5))
        );

        KeyedStream<StationLog, String> keyedStream = dsWithWatermark.keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog value) throws Exception {
                return value.sid;
            }
        });

        //2.定义匹配规则。 Pattern<T,F> : T:模式匹配中的事件类型，F:当前模式约束到的T的子类型
        Pattern<StationLog, StationLog> pattern = Pattern.<StationLog>begin("first")
                .where(new SimpleCondition<StationLog>() {
                    @Override
                    public boolean filter(StationLog value) throws Exception {
                        return value.callType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<StationLog>() {
                    @Override
                    public boolean filter(StationLog value) throws Exception {
                        return value.callType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<StationLog>() {
                    @Override
                    public boolean filter(StationLog value) throws Exception {
                        return value.callType.equals("fail");
                    }
                });

        //3.将匹配规则应用到数据流上
        PatternStream<StationLog> patternStream = CEP.pattern(keyedStream, pattern);

        //4.获取符合规则的数据
        SingleOutputStreamOperator<String> result = patternStream.process(
                new PatternProcessFunction<StationLog, String>() {
            /**
             * Map<模式名称，当前模式下匹配的事件>：各个模式下匹配到的事件
             * Context：上下文对象，可以获取时间相关的信息或者将数据输出到侧输出流
             * Collector：用于输出结果
             */
            @Override
            public void processMatch(Map<String, List<StationLog>> match, Context ctx, Collector<String> out) throws Exception {
                StationLog start = match.get("first").iterator().next();
                StationLog second = match.get("second").iterator().next();
                StationLog third = match.get("third").iterator().next();
                out.collect("预警信息：\n" + start + "\n" + second + "\n" + third);

            }
        });

        result.print();
        env.execute();

    }
}
