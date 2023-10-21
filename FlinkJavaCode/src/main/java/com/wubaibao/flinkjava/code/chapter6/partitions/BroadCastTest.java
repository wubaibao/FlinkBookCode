package com.wubaibao.flinkjava.code.chapter6.partitions;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink BroadCast Partition 测试
 * socket输入数据如下:
 * zs,100
 * ls,200
 * ww,300
 * ml,400
 * tq,500
 */
public class BroadCastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //用户分数信息
        SingleOutputStreamOperator<Tuple2<String, Integer>> mainDS = env.socketTextStream("node5", 9999).map(line -> {
            String[] arr = line.split(",");
            return Tuple2.of(arr[0], Integer.parseInt(arr[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //用户基本信息
        DataStreamSource<Tuple2<String, String>> sideDS = env.fromCollection(Arrays.asList(
                Tuple2.of("zs", "北京"),
                Tuple2.of("ls", "上海"),
                Tuple2.of("ww", "广州"),
                Tuple2.of("ml", "深圳"),
                Tuple2.of("tq", "杭州")));

        MapStateDescriptor<String, String> msd = new MapStateDescriptor<>("map-descriptor", String.class, String.class);

        //将用户基本信息广播出去
        BroadcastStream<Tuple2<String, String>> broadcast = sideDS.broadcast(msd);

        //连接两个流,并处理
        mainDS.connect(broadcast).process(new BroadcastProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>() {
            //处理主流数据
            @Override
            public void processElement(Tuple2<String, Integer> scoreInfo, BroadcastProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(msd);
                //获取用户基本信息
                String cityAddr = broadcastState.get(scoreInfo.f0);
                out.collect("姓名:"+scoreInfo.f0+",地址:"+ cityAddr + ",分数"+scoreInfo.f1);
            }

            //处理广播流数据
            @Override
            public void processBroadcastElement(Tuple2<String, String> baseInfo, BroadcastProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                //获取广播状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(msd);
                broadcastState.put(baseInfo.f0, baseInfo.f1);
            }
        }).print();

        env.execute();
    }
}
