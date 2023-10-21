package com.wubaibao.flinkjava.code.chapter8.window.globalwindow;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink 基于EventTime GlobalWindow 全局窗口测试
 * 案例：读取基站日志数据，手动指定trigger触发器，针对每个基站ID每3条数据触发一次计算。
 */
public class GlobalWindowWithKeyTest {
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
         * 003,181,183,busy,17000,50
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

        keyedStream.window(GlobalWindows.create())
                //自定义触发器，每3条数据触发一次计算
                .trigger(new MyCountTrigger())
                //自定义窗口函数，统计每个基站所有主叫通话总时长
                .process(new ProcessWindowFunction<StationLog, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<StationLog, String, String, GlobalWindow>.Context context,
                                        Iterable<StationLog> elements,
                                        Collector<String> out) throws Exception {
                        //统计每个基站近3个主叫通话总时长
                        long sumCallTime = 0L;
                        for (StationLog element : elements) {
                            sumCallTime += element.duration;
                        }

                        out.collect("基站：" + key + ",近3条通话总时长：" + sumCallTime);
                    }
                }).print();

        env.execute();
    }
}

//MyCountTrigger() 每隔3条数据触发一次计算
class MyCountTrigger extends Trigger<StationLog, GlobalWindow> {
    //设置 ValueStateDescriptor描述器，用于存储计数器
    private ValueStateDescriptor<Long> eventCountDescriptor = new ValueStateDescriptor<Long>("event-count", Long.class);

    //每来一条数据，都会调用一次
    @Override
    public TriggerResult onElement(StationLog element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        //获取状态计数器的值
        ValueState<Long> eventState = ctx.getPartitionedState(eventCountDescriptor);
        //每来一条数据，状态值加1，初始状态值为null,直接返回1即可
        Long count = eventState.value() == null ? 1L :eventState.value()+1L;
        //将计数器的值存入状态中
        eventState.update(count);

        //如果计数器的值等于3，触发计算，并清空计数器
        if (eventState.value() == 3L) {
            //清空状态计数
            eventState.clear();
            //触发计算
            return TriggerResult.FIRE_AND_PURGE;
        }

        //如果状态计数器的值不等于3，不触发计算
        return TriggerResult.CONTINUE;

    }

    //注册处理时间定时器。如果基于ProcessTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onProcessingTime方法
    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    //注册事件时间定时器。如果基于EventTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onEventTime方法
    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    //clear() 方法处理在对应窗口被移除时所需的逻辑。
    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(eventCountDescriptor).clear();
    }
}


