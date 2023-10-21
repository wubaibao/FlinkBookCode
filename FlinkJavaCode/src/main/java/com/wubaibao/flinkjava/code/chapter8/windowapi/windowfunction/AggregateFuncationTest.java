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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink AggregateFunction测试
 * 案例：读取基站日志数据，每隔5s统计每个基站通话总时长。
 */
public class AggregateFuncationTest {
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
                        .aggregate(new AggregateFunction<StationLog, Tuple2<String,Long>, String>() {
                            //创建累加器
                            @Override
                            public Tuple2<String, Long> createAccumulator() {
                                return Tuple2.of("",0L);
                            }

                            //累加器的累加逻辑
                            @Override
                            public Tuple2<String, Long> add(StationLog value, Tuple2<String, Long> accumulator) {
                                return Tuple2.of(value.sid,accumulator.f1 + value.duration);
                            }

                            //获取结果
                            @Override
                            public String getResult(Tuple2<String, Long> accumulator) {
                                return "基站："+accumulator.f0 + ",通话时长：" + accumulator.f1;
                            }

                            //合并累加器
                            @Override
                            public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                                return null;
                            }
                        }).print();

        env.execute();
    }
}
