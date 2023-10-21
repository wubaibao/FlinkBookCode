package com.wubaibao.flinkjava.code.chapter8.windowapi.windowfunction;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink WindowFunction测试
 * 案例：读取基站日志数据，每隔5s统计每个基站通话总时长。
 */
public class WindowFunctionTest {
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
        KeyedStream<Tuple2<String, Long>, String> keyedDStream = dsWithWatermark.map(new MapFunction<StationLog, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(StationLog stationLog) throws Exception {
                return Tuple2.of(stationLog.sid, stationLog.duration);
            }
        }).keyBy(t -> t.f0);

        keyedDStream
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                @Override
                public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                    long sum = 0L;
                    for (Tuple2<String, Long> tuple2 : input) {
                        sum += tuple2.f1;
                    }
                    out.collect("基站ID:" + key + "," +
                            "窗口范围:[" + window.getStart() + "," + window.getEnd() + "), 通话时长：" + sum);
                }
            }).print();
        env.execute();
    }
}
