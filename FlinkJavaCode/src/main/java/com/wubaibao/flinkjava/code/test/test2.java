package com.wubaibao.flinkjava.code.test;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.socketTextStream("node5", 9999);
        //将支字符串转化为一个对象类型
        SingleOutputStreamOperator<StationLog> ds = source.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String line) throws Exception {
                String[] strings = line.split(",");
                return new StationLog(strings[0], strings[1], strings[2], strings[3], Long.valueOf(strings[4]), Long.valueOf(strings[5]));
            }
        });
        //1.使用KeyedState状态标识，首先需要产生一个KeyedStream
        KeyedStream<StationLog, String> stream = ds.keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog stationLog) throws Exception {
                return stationLog.callOut;
            }
        });
        stream.map(new RichMapFunction<StationLog, String>() {
            //定义状态描述器和获取状态描述器
            private MapState<String, Long> stringLongMapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                //每个key都有自己的状态标识
                MapStateDescriptor<String, Long> mapState = new MapStateDescriptor<String, Long>("mapState", String.class, Long.class);
                stringLongMapState = getRuntimeContext().getMapState(mapState);
            }
            @Override
            public String map(StationLog stationLog) throws Exception {
                //获取主叫号码对应的被叫号码
                String callIn = stationLog.callIn;
                //获取主叫号码对应的被叫的通话时长
                Long duration = stationLog.duration;

                if(stringLongMapState.get(callIn) != null){
                    System.out.println("stringLongMapState.get(callIn) : " +stringLongMapState.get(callIn));
                    System.out.println("duration = "+duration);
                    //定义状态描述器中被叫号码存在,则更新通话时长
                    Long time = stringLongMapState.get("callIn")+duration;
                    stringLongMapState.put(callIn,time);
                }else{
                    //定义状态描述器中被叫号码不存在，将被叫号码，和通话时长加入到状态描述器中
                    stringLongMapState.put(callIn,duration);
                }
                String info = "";
                for(String key:stringLongMapState.keys()){
                    info += "被叫号码:"+key+"，对应的通话总时长为:"+stringLongMapState.get(key);
                }
                return "主叫号码:"+stationLog.callOut+info;
            }
        }).print();
        environment.execute();
    }
}
