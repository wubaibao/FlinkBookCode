package com.wubaibao.flinkjava.code.chapter8.windowapi.sideoutputlatedata;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Flink - allowedLateness()允许迟到数据案例
 * 案例：读取socket基站日志数据，每隔5s统计每个基站通话总时长。
 * 设置watermark的延迟时间为2秒，同时设置allowedLateness允许延迟时间为2秒，将迟到严重的数据通过侧输出流方式进行收集。
 */
public class SideOutputLateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        //定义侧输出流标签，最后必须是“{}”形式，避免类型擦除
        OutputTag<StationLog> lateOutputTag = new OutputTag<StationLog>("late-data"){};

        //每隔5s统计每个基站所有主叫通话总时长，使用事件时间
        SingleOutputStreamOperator<String> result = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //watermark 基础之上，再延迟2s
                .allowedLateness(Time.seconds(2))
                //迟到的数据，通过侧输出流方式进行收集
                .sideOutputLateData(lateOutputTag)
                .process(new ProcessWindowFunction<StationLog, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<StationLog, String, String, TimeWindow>.Context context,
                                        Iterable<StationLog> elements,
                                        Collector<String> out) throws Exception {
                        //统计每个基站所有主叫通话总时长
                        long sumCallTime = 0L;
                        for (StationLog element : elements) {
                            sumCallTime += element.duration;
                        }

                        //获取窗口起始时间
                        long start = context.window().getStart() <0 ? 0 : context.window().getStart();
                        long end = context.window().getEnd();

                        out.collect("窗口范围：[" + start + "~" + end + "),基站：" + key + ",所有主叫通话总时长：" + sumCallTime);
                    }
                });

        result.print("正常窗口数据");
        //获取迟到数据
        result.getSideOutput(lateOutputTag).print("迟到数据");

        env.execute();

    }
}
