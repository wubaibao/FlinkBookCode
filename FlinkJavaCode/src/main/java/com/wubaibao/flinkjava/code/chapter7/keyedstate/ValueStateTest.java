package com.wubaibao.flinkjava.code.chapter7.keyedstate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink ValueState 状态编程
 * 案例: 读取基站日志数据，统计每个主叫手机通话间隔时间，单位为毫秒
 */
public class ValueStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Socket中数据如下:
         *  001,186,187,busy,1000,10
         *  002,187,186,fail,2000,20
         *  003,186,188,busy,3000,30
         *  004,187,186,busy,4000,40
         *  005,189,187,busy,5000,50
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //对ds进行转换处理，得到StationLog对象
        SingleOutputStreamOperator<StationLog> stationLogDS = ds.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String line) throws Exception {
                String[] arr = line.split(",");
                return new StationLog(
                        arr[0].trim(),
                        arr[1].trim(),
                        arr[2].trim(),
                        arr[3].trim(),
                        Long.valueOf(arr[4]),
                        Long.valueOf(arr[5])
                );
            }
        });

        SingleOutputStreamOperator<String> result = stationLogDS.keyBy(stationLog -> stationLog.callOut).process(new KeyedProcessFunction<String, StationLog, String>() {
            //定义 ValueState
            private ValueState<Long> callTimeValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义状态描述器
                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("callTimeValueState", Long.class);
                //获取状态
                callTimeValueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(StationLog stationLog, KeyedProcessFunction<String, StationLog, String>.Context ctx, Collector<String> out) throws Exception {
                //获取状态
                Long callTime = callTimeValueState.value();
                if (callTime == null) {
                    //如果状态为空，说明是第一次通话，直接将当前通话时间存入状态
                    callTimeValueState.update(stationLog.callTime);
                } else {
                    //如果状态不为空，说明不是第一次通话，计算两次通话间隔时间
                    long intervalTime = stationLog.callTime - callTime;
                    out.collect("主叫手机号为：" + stationLog.callOut + "，通话间隔时间为：" + intervalTime);
                    //更新状态
                    callTimeValueState.update(stationLog.callTime);
                }
            }
        });

        result.print();
        env.execute();
    }
}
