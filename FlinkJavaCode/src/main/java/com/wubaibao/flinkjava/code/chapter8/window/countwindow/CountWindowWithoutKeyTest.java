package com.wubaibao.flinkjava.code.chapter8.window.countwindow;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
/**
 *  CountWindow 计数窗口测试
 *  案例：读取基站日志数据，所有基站ID每5条数据触发一次计算。
 */
public class CountWindowWithoutKeyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * Socket中输入数据格式如下：
         * 001,181,182,busy,1000,10
         * 002,182,183,fail,3000,20
         * 001,183,184,busy,2000,30
         * 002,184,185,busy,6000,40
         * 001,181,183,busy,5000,50
         * 001,181,182,busy,7000,10
         * 002,182,183,fail,9000,20
         * 001,183,184,busy,11000,30
         * 002,184,185,busy,6000,40
         * 002,181,183,busy,12000,50
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

        dsWithWatermark.countWindowAll(5)
                //自定义窗口函数，统计每个基站所有主叫通话总时长
                .process(new ProcessAllWindowFunction<StationLog, String, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<StationLog, String, GlobalWindow>.Context context, Iterable<StationLog> elements, Collector<String> out) throws Exception {
                        //统计每个基站所有主叫通话总时长
                        long sumCallTime = 0L;
                        for (StationLog element : elements) {
                            sumCallTime += element.duration;
                        }

                        out.collect("所有基站，近5条通话总时长：" + sumCallTime);
                    }

                }).print();

        env.execute();
    }
}