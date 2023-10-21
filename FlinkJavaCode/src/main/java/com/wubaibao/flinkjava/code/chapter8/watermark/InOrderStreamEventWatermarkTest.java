package com.wubaibao.flinkjava.code.chapter8.watermark;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink 有序数据流Watermark设置
 * 案例：读取Socket基站日志数据，按照基站id进行分组，每隔5s窗口统计每个基站所有主叫通话总时长
 */
public class InOrderStreamEventWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        /**
         * Socket中输入数据格式如下：
         * 001,181,182,busy,1000,10
         * 002,182,183,fail,2000,20
         * 003,183,184,busy,3000,30
         * 004,184,185,busy,4000,40
         * 005,181,183,busy,5000,50
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

        //给 stationLogDS 流设置watermark
//        stationLogDS.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<StationLog>forMonotonousTimestamps()
//                        .withTimestampAssigner((stationLog, timestamp) -> stationLog.callTime));
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<StationLog>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<StationLog>() {
                            //stationLog是输入的数据，timestamp是当前元素时间戳，如果没有分配过时间戳，默认值为Long.MIN_VALUE，即-9223372036854775808
                            @Override
                            public long extractTimestamp(StationLog stationLog, long timestamp) {
                                return stationLog.callTime;
                            }
                        })

                        /**
                         * 多个并行度时，如果某个并行度长时间没有数据，会导致watermark不会推进，
                         * 这时可以设置一个最大的空闲时间，如果超过这个时间，watermark就会推进，
                         * 该时间是基于当前机器的系统时间来计时
                         */
                        .withIdleness(Duration.ofSeconds(5))
        );

        //按照 基站id 进行分组，每隔5s窗口统计每个基站所有主叫通话总时长
        dsWithWatermark.keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog stationLog) throws Exception {
                return stationLog.sid;
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("duration")
                .print();

        env.execute();
    }
}
