package com.wubaibao.flinkjava.code.chapter7.keyedstate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink MapState 状态编程
 * 案例：读取基站日志数据，统计主叫号码呼出的全部被叫号码及对应被叫号码通话总时长。
 */
public class MapStateTest {
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
                .map(new RichMapFunction<StationLog, String>() {
                    //定义一个MapState 用来存放同一个主叫号码的所有被叫号码和对应的通话时长
                    private MapState<String, Long> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义状态描述器
                        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("map-state",
                                String.class, Long.class);
                        //获取状态
                        mapState = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public String map(StationLog stationLog) throws Exception {
                        String callIn = stationLog.callIn;
                        Long duration = stationLog.duration;

                        //从状态中获取当前主叫号码的被叫号码的通话时长，再设置回状态
                        if (mapState.get(callIn) != null) {
                            duration += mapState.get(callIn);
                            mapState.put(callIn, duration);
                        } else {
                            mapState.put(callIn, duration);
                        }

                        //遍历状态中的数据，将数据拼接成字符串返回
                        String info = "";
                        for (String key : mapState.keys()) {
                            info += "被叫：" + key + "，通话总时长：" + mapState.get(key) + "->";
                        }
                        return info;
                    }
                });

        result.print();
        env.execute();
    }
}
