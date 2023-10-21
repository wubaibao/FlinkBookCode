package com.wubaibao.flinkjava.code.chapter8.windowapi.windowfunction;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink AggregateFunction测试
 * 案例:读取socket基站日志数据，每隔5s统计每个基站平均通话时长
 */
public class AggregateFunctionAndProcessWindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * Socket中输入数据格式如下：
         * 001,181,182,busy,1000,10
         * 002,182,183,fail,3000,20
         * 001,183,184,busy,2000,30
         * 002,184,185,busy,6000,40
         * 003,181,183,busy,5000,50
         * 001,181,182,busy,7000,10
         * 002,182,183,fail,9000,20
         * 001,183,184,busy,11000,30
         * 002,184,185,busy,6000,40
         * 003,181,183,busy,12000,50
         *
         */
        DataStreamSource<String> sourceDS = env.socketTextStream("node5", 9999);

        //将数据转换成StationLog对象
        SingleOutputStreamOperator<StationLog> stationLogDS = sourceDS.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String s) throws Exception {
                String[] arr = s.split(",");
                return new StationLog(arr[0].trim(),
                        arr[1].trim(),
                        arr[2].trim(),
                        arr[3].trim(),
                        Long.valueOf(arr[4]),
                        Long.valueOf(arr[5]));
            }
        });

        //设置水位线
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((stationLog, timestamp) -> stationLog.callTime)
                        //设置并行度空闲时间，方便推进水位线
                        .withIdleness(Duration.ofSeconds(5))
        );

        //按照基站ID进行分组，并每隔5s统计每个基站所有主叫通话总时长
        KeyedStream<StationLog, String> keyedStream = dsWithWatermark.keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog stationLog) throws Exception {
                return stationLog.sid;
            }
        });

        //每隔5s统计每个基站所有主叫通话总时长，使用事件时间
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<StationLog, Tuple2<Long, Long>, Tuple2<Long, Long>>() {

                    //创建累加器
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L, 0L);
                    }

                    //累加器的累加逻辑
                    @Override
                    public Tuple2<Long, Long> add(StationLog value, Tuple2<Long, Long> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.duration, accumulator.f1 + 1L);
                    }

                    //获取结果
                    @Override
                    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
                        return accumulator;
                    }

                    //累加器的合并逻辑
                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Tuple2<Long, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<Tuple2<Long, Long>, String, String, TimeWindow>.Context context,
                                        Iterable<Tuple2<Long, Long>> elements,
                                        Collector<String> out) throws Exception {
                        //获取累加器中的结果
                        Tuple2<Long, Long> tuple2 = elements.iterator().next();
                        out.collect("基站ID:" + key + "," +
                                "窗口范围:[" + context.window().getStart() + "," + context.window().getEnd() + ")," +
                                "平均通话时长：" + Double.valueOf(tuple2.f0 / tuple2.f1));
                    }

                }).print();

        env.execute();
    }
}
