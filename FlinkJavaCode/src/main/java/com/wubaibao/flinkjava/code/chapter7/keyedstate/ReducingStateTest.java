package com.wubaibao.flinkjava.code.chapter7.keyedstate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink ReducingState 状态编程测试
 * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长
 */
public class ReducingStateTest {
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

        SingleOutputStreamOperator<String> result = stationLogDS.keyBy(stationLog -> stationLog.callOut)
                .process(new KeyedProcessFunction<String, StationLog, String>() {
                    //定义 ReducingState 状态，存储通话总时长
                    private ReducingState<Long> callDurationState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义状态描述器
                        ReducingStateDescriptor<Long> rsd = new ReducingStateDescriptor<Long>("callDuration-state",
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long value1, Long value2) throws Exception {
                                        return value1 + value2;
                                    }
                                }, Long.class);

                        //根据状态描述器获取状态
                        callDurationState = getRuntimeContext().getReducingState(rsd);
                    }

                    @Override
                    public void processElement(StationLog stationLog, KeyedProcessFunction<String, StationLog, String>.Context ctx, Collector<String> out) throws Exception {

                        //获取当前主叫号码状态中的通话时长
                        Long totalCallTime = callDurationState.get();

                        if (totalCallTime == null) {
                            //获取当前处理数据时间
                            long time = ctx.timerService().currentProcessingTime();
                            //注册一个20s后的定时器
                            ctx.timerService().registerProcessingTimeTimer(time + 20 * 1000L);
                        }

                        //将通话时长存入ListState
                        callDurationState.add(stationLog.duration);

                    }

                    /**
                     * 定时器触发时，调用该方法
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, StationLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        //输出结果
                        out.collect("主叫号码：" + ctx.getCurrentKey() + "，近20s通话时长：" + callDurationState.get());

                        //清空状态
                        callDurationState.clear();
                    }
                });
        result.print();
        env.execute();

    }
}
