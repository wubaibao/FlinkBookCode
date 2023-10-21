package com.wubaibao.flinkjava.code.chapter8.eventtimedsoperator;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink EventTime下union操作
 * 案例：读取socket中数据流形成两个流，进行关联后设置窗口，每隔5秒统计每个基站通话次数。
 */
public class UnionOnEventTimeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //方便测试，并行度设置为1
        env.setParallelism(1);
        /**
         * 读取socket中数据流形成A流，并对A流设置watermark
         */
        SingleOutputStreamOperator<StationLog> ADS = env.socketTextStream("node5", 8888)
                .map(new MapFunction<String, StationLog>() {
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
        SingleOutputStreamOperator<StationLog> AdsWithWatermark = ADS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((stationLog, timestamp) -> stationLog.callTime)
        );

        /**
         * 读取socket中数据流形成B流,并对B流设置watermark
         */
        SingleOutputStreamOperator<StationLog> BDS = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, StationLog>() {
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
        SingleOutputStreamOperator<StationLog> BdsWithWatermark = BDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((stationLog, timestamp) -> stationLog.callTime)
        );


        //两流进行union操作
        AdsWithWatermark.union(BdsWithWatermark)
                .keyBy(new KeySelector<StationLog, String>() {
                    @Override
                    public String getKey(StationLog stationLog) throws Exception {
                        return stationLog.sid;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<StationLog, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<StationLog, String, String, TimeWindow>.Context context,
                                        Iterable<StationLog> elements,
                                        Collector<String> out) throws Exception {
                        System.out.println("window-watermark:" + context.currentWatermark());
                        //获取窗口起始时间
                        long start = context.window().getStart()<0?0:context.window().getStart();
                        //获取窗口结束时间
                        long end = context.window().getEnd();
                        //统计窗口内通话次数
                        int count = 0;
                        for (StationLog element : elements) {
                            count++;
                        }
                        out.collect("窗口范围：[" + start + "~" + end + "),基站：" + key + ",通话总次数为：" + count);
                    }
                }).print();

        env.execute();
    }
}
