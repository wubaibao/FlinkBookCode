package com.wubaibao.flinkjava.code.chapter8.watermark;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Flink watermark 对齐测试
 *  Socket 中输入数据格式如下：
 *  001,181,182,busy,1000,10
 *  001,184,185,busy,2000,40
 *  001,181,183,busy,3000,50
 *  001,182,183,fail,4000,20
 *  001,181,185,success,5000,60
 */
public class WatermarkAlignmentTest {
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

        //给 stationLogDS 流设置Watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //设置Watermark，最大延迟时间为5s
                WatermarkStrategy.<StationLog>forMonotonousTimestamps()
                        //设置EventTime对应字段
                        .withTimestampAssigner(new SerializableTimestampAssigner<StationLog>() {
                            @Override
                            public long extractTimestamp(StationLog element, long recordTimestamp) {
                                return element.callTime;
                            }
                        })
                        //设置Watermark对齐,对齐组为socket-source-group，watermark最大偏移值为5s，检查周期是2s
                        .withWatermarkAlignment("socket-source-group",Duration.ofSeconds(5),
                                Duration.ofSeconds(2))
        );

        dsWithWatermark.print();

        env.execute();
    }
}
