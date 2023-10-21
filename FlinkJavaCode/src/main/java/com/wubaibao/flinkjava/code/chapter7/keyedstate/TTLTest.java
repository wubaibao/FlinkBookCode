package com.wubaibao.flinkjava.code.chapter7.keyedstate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink KeyedState TTL测试
 * 案例：读取Socket中基站通话数据，统计每个主叫通话总时长
 */
public class TTLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Socket中数据如下:
         *  001,186,187,busy,1000,10
         *  002,187,186,fail,2000,20
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

        stationLogDS.keyBy(stationLog -> stationLog.callOut)
                .map(new RichMapFunction<StationLog, String>() {
                    //定义ValueState 用来存放同一个主叫号码的通话总时长
                    private ValueState<Long> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义状态TTL
                        StateTtlConfig ttlConfig = StateTtlConfig
                                //设置状态有效期为10秒
                                .newBuilder(org.apache.flink.api.common.time.Time.seconds(10))
                                //设置状态更新类型为OnCreateAndWrite，即状态TTL创建和写入时更新
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                //设置状态可见性为NeverReturnExpired，即状态过期后不返回
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();

                        //定义状态描述器
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("value-state", Long.class);

                        //设置状态TTL
                        descriptor.enableTimeToLive(ttlConfig);

                        //获取状态
                        valueState = getRuntimeContext().getState(descriptor);

                    }

                    @Override
                    public String map(StationLog stationLog) throws Exception {
                        Long stateValue = valueState.value();
                        if(stateValue==null){
                            //如果状态值为null，说明是第一次使用，直接更新状态值
                            valueState.update(stationLog.duration);
                        }else{
                            //如果状态值不为null，说明不是第一次使用，需要累加通话时长
                            valueState.update(stateValue+stationLog.duration);
                        }

                        return stationLog.callOut+"通话总时长："+valueState.value()+"秒";
                    }
                }).print();

        env.execute();
    }
}
