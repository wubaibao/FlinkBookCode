package com.wubaibao.flinkjava.code.chapter8.watermark;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink 自定义Watermark - PeriodicWatermarkGenerator
 * 案例：读取Socket基站日志数据，按照基站id进行分组，每隔5s窗口统计每个基站所有主叫通话总时长
 */
public class PeriodicWatermarkGeneratorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        //设置watermark生成周期为100ms
//        env.getConfig().setAutoWatermarkInterval(100);

        /**
         * Socket中输入数据格式如下：
         * 001,181,182,busy,1000,10
         * 004,184,185,busy,4000,40
         * 005,181,183,busy,5000,50
         * 002,182,183,fail,2000,20
         * 001,181,185,success,6000,60
         * 003,182,184,busy,7000,30
         * 003,183,181,busy,3000,30
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

        //给stationLogDS 流设置Watermark ，这里使用自定义watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //使用自定义 Periodic watermark
                WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<StationLog>() {
                    @Override
                    public WatermarkGenerator<StationLog> createWatermarkGenerator(Context context) {
                        return new CustomPeriodicWatermark();
                    }
                })
                //从事件中抽取时间戳作为事件时间
                .withTimestampAssigner(new SerializableTimestampAssigner<StationLog>() {
                    @Override
                    public long extractTimestamp(StationLog stationLog, long l) {
                        return stationLog.callTime;
                    }
                })
                //设置并行度空闲时间，方便推进水位线
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

class CustomPeriodicWatermark implements WatermarkGenerator<StationLog> {
    //定义最大延迟时间
    long maxOutOfOrderness  = 2000;

    //定义当前最大时间戳,初始值为最小值+最大延迟时间+1，为什么要加1？因为假设当前watermark为Long.MIN_VALUE,那么watermark = currentMaxTimestamp - maxDelay - 1,所以 currentMaxTimestamp = Long.MIN_VALUE + maxDelay + 1
    long currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness  + 1;

    /**
     * 每来一条数据，调用一次，更新最大时间戳
     */
    @Override
    public void onEvent(StationLog stationLog, long eventTimestamp, WatermarkOutput output) {
        //更新最大时间戳
        currentMaxTimestamp = Math.max(currentMaxTimestamp, stationLog.callTime);
    }

    /**
     * 周期性的调用，生成新的Watermark
     * 调用此方法生成 watermark 的间隔时间由env.getConfig().setAutoWatermarkInterval(100)设置，默认是200ms
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        //生成Watermark，这里为什么要减1?,假设当前watermark时间为t,代表时间戳<=t的数据都已经到达了，此刻有可能后续还会来一个时间戳为t的数据，所以要减1，代表时间戳<t的数据都已经到达了
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness  - 1));
    }
}
