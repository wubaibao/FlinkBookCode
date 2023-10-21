package com.wubaibao.flinkjava.code.chapter8.windowapi.evictor;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * 用户自定义Evictor
 * 案例：读取基站日志数据，手动指定trigger触发器，每个基站数据隔5秒生成窗口并触发计算。
 * (根据watermark来看超时的数据不再计算到窗口内)
 */
public class CustomEvidtorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * Socket中输入数据格式如下：
         * 001,181,182,busy,1000,10
         * 002,182,183,fail,3000,20
         * 001,183,184,busy,2000,30
         * 002,184,185,busy,6000,40
         * #该条数据输入SID 001 对应5秒后窗口触发
         * 003,181,183,busy,8000,50
         * #继续输入SID 001 数据，重新注册定时器，5秒后触发
         * 001,181,182,busy,7000,10
         * #此刻wm为5999，如果有超时数据过来，数据会被剔除
         * 001,181,184,busy,1000,10
         * 001,182,185,busy,2000,20
         * 001,183,186,busy,3000,30
         * #此条数据输入后，wm为11999，SID 002 对应窗口会触发，另外SID 001窗口也会触发
         * 003,181,187,busy,14000,10
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
                //自定义触发器，每个事件5秒后触发一次计算
                .trigger(new MyTimeTriggerCls())
                //自定义移除器，移除迟到数据
                .evictor(new Evictor<StationLog, GlobalWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<StationLog>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        Iterator<TimestampedValue<StationLog>> iter = elements.iterator();
                        //如果数据的 callType 标记为"迟到数据"，则移除该数据
                        while (iter.hasNext()) {
                            TimestampedValue<StationLog> next = iter.next();
                            if (next.getValue().callType.equals("迟到数据")) {
                                System.out.println("移除了迟到数据：" + next.getValue());
                                //移除迟到数据,删除当前指针所指向的元素
                                iter.remove();
                            }
                        }

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<StationLog>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

                    }
                })
                //自定义窗口函数，统计每个基站所有主叫通话总时长
                .process(new ProcessWindowFunction<StationLog, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<StationLog, String, String, GlobalWindow>.Context context,
                                        Iterable<StationLog> elements,
                                        Collector<String> out) throws Exception {
                        //统计每个基站主叫通话总时长
                        long sumCallTime = 0L;
                        for (StationLog element : elements) {
                            sumCallTime += element.duration;
                        }

                        out.collect("基站：" + key + ",所有主叫通话总时长：" + sumCallTime);
                    }
                }).print();

        env.execute();

    }
}


//MyTimeTrigger1() 针对每个事件每5秒触发一次计算
class MyTimeTriggerCls extends Trigger<StationLog, GlobalWindow> {

    //创建状态描述符，该状态标记当前key是否有对应的定时器
    private ValueStateDescriptor<Boolean> timerStateDescriptor = new ValueStateDescriptor<>("timer-state", Boolean.class);

    //每来一条数据，都会调用一次
    @Override
    public TriggerResult onElement(StationLog element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onElement >>>>>>>>>>>>>>>> 方法调用了,当前事件时间"+timestamp+",当前水位线"+ctx.getCurrentWatermark());
        //获取当前窗口中定时器是否存在的状态
        Boolean isExist = ctx.getPartitionedState(timerStateDescriptor).value();

        if(isExist == null || !isExist){
            System.out.println("注册定时器，触发时间：" + (timestamp + 4999));
            //注册一个基于事件时间的定时器，延迟5秒触发
            ctx.registerEventTimeTimer(timestamp + 4999L);
            //更新状态
            ctx.getPartitionedState(timerStateDescriptor).update(true);
        }

        //如果事件时间小于了watermark，说明迟到了，给该数据做个标记
        if(timestamp < ctx.getCurrentWatermark()){
            element.setCallType("迟到数据");
        }

        return TriggerResult.CONTINUE;
    }

    //注册处理时间定时器。如果基于ProcessTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onProcessingTime方法
    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onProcessingTime >>>>>>>>>>>>>>>> 方法调用了");
        //不使用处理时间，这里直接返回CONTINUE
        return TriggerResult.CONTINUE;
    }

    /**
     * 注册事件时间定时器。如果基于EventTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onEventTime方法
     * @param time 定时器触发时间
     */
    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onEventTime >>>>>>>>>>>>>>>> 方法调用了，触发器执行，触发时间：" + time);
        //更新状态为false
        ctx.getPartitionedState(timerStateDescriptor).update(false);

        return TriggerResult.FIRE_AND_PURGE;
    }

    //clear() 方法处理在对应窗口被移除时所需的逻辑。
    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        System.out.println("clear >>>>>>>>>>>>>>>> 方法调用了，清空状态");
        ctx.getPartitionedState(timerStateDescriptor).clear();
    }
}


