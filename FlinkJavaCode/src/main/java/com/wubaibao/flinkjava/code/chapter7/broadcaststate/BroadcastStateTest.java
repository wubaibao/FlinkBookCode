package com.wubaibao.flinkjava.code.chapter7.broadcaststate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import com.wubaibao.flinkjava.code.chapter7.PersonInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink BroadcastState 状态编程
 * 案例：读取两个Socket中的数据，一个是用户信息，一个是通话日志，
 *  将通话日志中的主叫号码和被叫号码与用户信息中的手机号码进行匹配，将用户信息补充到通话日志中。
 *  使用广播状态实现。
 */
public class BroadcastStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 获取主流数据：通话日志
         * Socket 数据格式如下:
         * 001,181,182,busy,1000,10
         * 002,182,183,fail,2000,20
         * 003,183,184,busy,3000,30
         * 004,184,185,busy,4000,40
         * 005,181,183,busy,5000,50
         */
        SingleOutputStreamOperator<StationLog> mainDS = env.socketTextStream("node5", 9999).map(new MapFunction<String, StationLog>() {
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

        /**
         * 获取用户信息流：用户信息
         * Socket 数据格式如下:
         *  181,张三,北京
         *  182,李四,上海
         *  183,王五,广州
         *  184,赵六,深圳
         *  185,田七,杭州
         */
        SingleOutputStreamOperator<PersonInfo> personInfoDS = env.socketTextStream("node5", 8888).map(new MapFunction<String, PersonInfo>() {
            @Override
            public PersonInfo map(String s) throws Exception {
                String[] arr = s.split(",");
                return new PersonInfo(arr[0].trim(), arr[1].trim(), arr[2].trim());
            }
        });

        //定义广播状态描述器
        MapStateDescriptor<String, PersonInfo> mapStateDescriptor = new MapStateDescriptor<>("mapState",
                String.class,
                PersonInfo.class);

        //将用户信息流广播出去
        BroadcastStream<PersonInfo> personInfoBroadcastStream = personInfoDS.broadcast(mapStateDescriptor);
        //将主流和广播流进行连接
        SingleOutputStreamOperator<String> result = mainDS.connect(personInfoBroadcastStream)
                .process(new BroadcastProcessFunction<StationLog, PersonInfo, String>() {
                    //处理主流数据
                    @Override
                    public void processElement(StationLog stationLog, BroadcastProcessFunction<StationLog, PersonInfo, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //获取广播状态中的数据
                        PersonInfo callOutPersonInfo = ctx.getBroadcastState(mapStateDescriptor).get(stationLog.callOut);
                        PersonInfo callInPersonInfo = ctx.getBroadcastState(mapStateDescriptor).get(stationLog.callIn);

                        String callOutName = callOutPersonInfo != null ? callOutPersonInfo.name : "";
                        String callOutCity = callOutPersonInfo != null ? callOutPersonInfo.city : "";
                        String callInName = callInPersonInfo != null ? callInPersonInfo.name : "";
                        String callInCity = callInPersonInfo != null ? callInPersonInfo.city : "";

                        //输出数据
                        out.collect("主叫姓名：" + callOutName + "，" +
                                "主叫城市：" + callOutCity + "，" +
                                "被叫姓名：" + callInName + "，" +
                                "被叫城市：" + callInCity + "，" +
                                "通话状态：" + stationLog.callType + "，" +
                                "通话时长：" + stationLog.duration);
                    }

                    //处理广播流数据
                    @Override
                    public void processBroadcastElement(PersonInfo personInfo, BroadcastProcessFunction<StationLog, PersonInfo, String>.Context ctx, Collector<String> out) throws Exception {
                        ctx.getBroadcastState(mapStateDescriptor).put(personInfo.phoneNum, personInfo);
                    }
                });

        result.print();
        env.execute();

    }
}
