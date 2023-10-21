package com.wubaibao.flinkjava.code.chapter6.processfun;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink ProcessFunction 测试
 * 案例：Flink读取Socket中通话数据，如果被叫手机连续5s呼叫失败生成告警信息
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //必须设置checkpoint，否则数据不能正常写出到mysql
        env.enableCheckpointing(5000);
        /**
         * socket 中输入数据如下：
         * 001,186,187,fail,1000,10
         * 002,186,187,success,2000,20
         * 003,187,188,fail,3000,30
         * 004,187,188,fail,4000,40
         */
        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map(one -> {
                    String[] arr = one.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });


        //按照被叫号码分组
        KeyedStream<StationLog, String> keyedStream = ds.keyBy(stationLog -> stationLog.getCallIn());

        //使用ProcessFunction实现通话时长超过5秒的告警
        keyedStream.process(new KeyedProcessFunction<String, StationLog, String>() {
            //使用状态记录上一次通话时间
            ValueState<Long> timeState = null;

            //在open方法中初始化记录时间的状态
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> time = new ValueStateDescriptor<>("time", Long.class);
                timeState = getRuntimeContext().getState(time);
            }

            //每来一条数据，调用一次
            @Override
            public void processElement(StationLog value, KeyedProcessFunction<String, StationLog, String>.Context ctx, Collector<String> out) throws Exception {
                //从状态中获取上次状态存储时间
                Long time = timeState.value();
                //如果时间为null，说明是第一条数据，注册定时器
                if("fail".equals(value.callType) && time == null){
                    //获取当前时间
                    long nowTime = ctx.timerService().currentProcessingTime();
                    //注册定时器，5秒后触发
                    long onTime = nowTime + 5000;
                    ctx.timerService().registerProcessingTimeTimer(onTime);
                    //更新状态
                    timeState.update(onTime);
                }

                // 表示有呼叫成功了，可以取消触发器
                if (!value.callType.equals("fail") && time != null) {
                    ctx.timerService().deleteProcessingTimeTimer(time);
                    timeState.clear();
                }

            }

            //定时器触发时调用,执行触发器，发出告警
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, StationLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("触发时间:" + timestamp + " 被叫手机号：" + ctx.getCurrentKey() +" 连续5秒呼叫失败！");
                //清空时间状态
                timeState.clear();
            }
        }).print();

        env.execute();

    }
}
