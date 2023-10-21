package com.wubaibao.flinkjava.code.chapter8.eventtimedsoperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 案例：读取订单流和支付流，超过一定时间订单没有支付进行报警提示。
 */
public class ConnectOnEventTimeTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //方便测试，并行度设置为1
        env.setParallelism(1);
        /**
         * 读取socket中订单流，并对订单流设置watermark
         * 订单流数据格式:订单ID,用户ID,订单金额,时间戳
         * order1,user_1,10,1000
         * order2,user_2,20,2000
         * order3,user_3,30,3000
         */
        SingleOutputStreamOperator<String> orderDS = env.socketTextStream("node5", 8888);
        //设置水位线
        SingleOutputStreamOperator<String> orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((orderInfo, timestamp) -> Long.valueOf(orderInfo.split(",")[3]))
        );

        /**
         * 读取socket中支付流，并对支付流设置watermark
         * 支付流数据格式:订单ID,支付金额,时间戳
         * order1,10,1000
         * order2,20,2000
         * order3,30,3000
         */
        SingleOutputStreamOperator<String> payDS = env.socketTextStream("node5", 9999);
        //设置水位线
        SingleOutputStreamOperator<String> payDSWithWatermark = payDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((payInfo, timestamp) -> Long.valueOf(payInfo.split(",")[2]))
        );

        //将订单流和支付流进行关联
        orderDSWithWatermark.keyBy(orderInfo -> orderInfo.split(",")[0])
                .connect(payDSWithWatermark.keyBy(payInfo -> payInfo.split(",")[0]))
                .process(new KeyedCoProcessFunction<String, String, String, String>() {
                    //订单状态，存储订单信息
                    private ValueState<String> orderState=null;
                    //支付状态，存储支付信息
                    private ValueState<String> payState=null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义两个状态，一个用来存放订单信息，一个用来存放支付信息
                        ValueStateDescriptor<String> orderStateDescriptor =
                                new ValueStateDescriptor<>("order-state", String.class);
                        ValueStateDescriptor<String> payStateDescriptor =
                                new ValueStateDescriptor<>("pay-state", String.class);

                        orderState = getRuntimeContext().getState(orderStateDescriptor);
                        payState = getRuntimeContext().getState(payStateDescriptor);
                    }

                    //处理订单流
                    @Override
                    public void processElement1(String orderInfo,
                                                KeyedCoProcessFunction<String, String, String, String>.Context ctx,
                                                Collector<String> out) throws Exception {

                        //当来一条订单数据后，判断支付状态是否为空，如果为空，说明订单没有支付，注册定时器，5秒后提示
                        if (payState.value() == null) {
                            //获取订单时间戳
                            long orderTimestamp = Long.valueOf(orderInfo.split(",")[3]);
                            //注册定时器，设置定时器触发时间延后5s触发
                            ctx.timerService().registerEventTimeTimer(orderTimestamp + 5 * 1000L);
                            //更新当前订单状态
                            orderState.update(orderInfo);
                        }else{
                            //如果支付状态不为空，说明订单已经支付，删除定时器
                            //获取定时器触发的时间
                            Long triggerTime = Long.valueOf(payState.value().split(",")[2])+5*1000L;
                            //删除定时器
                            ctx.timerService().deleteEventTimeTimer(triggerTime);
                            //删除完支付状态中的定时器后，清空支付状态
                            payState.clear();
                        }


                    }

                    //处理支付流
                    @Override
                    public void processElement2(String payInfo,
                                                KeyedCoProcessFunction<String, String, String, String>.Context ctx,
                                                Collector<String> out) throws Exception {
                        //当来一条支付数据后，判断订单状态是否为空，如果为空，说明订单没有支付，注册定时器，5秒后提示
                        if (orderState.value() == null) {
                            //获取支付时间戳
                            long payTimestamp = Long.valueOf(payInfo.split(",")[2]);
                            //注册定时器,设置定时器触发时间延后5s触发
                            ctx.timerService().registerEventTimeTimer(payTimestamp + 5 * 1000L);
                            //更新当前支付状态
                            payState.update(payInfo);
                        }else{
                            //如果订单状态不为空，说明订单已经支付，删除定时器
                            //获取定时器触发的时间
                            Long triggerTime = Long.valueOf(orderState.value().split(",")[3])+5*1000L;
                            //删除定时器
                            ctx.timerService().deleteEventTimeTimer(triggerTime);
                            //删除完订单状态中的定时器后，清空订单状态
                            orderState.clear();
                        }

                    }

                    //定时器触发后，执行的方法
                    @Override
                    public void onTimer(long timestamp,
                                        KeyedCoProcessFunction<String, String, String, String>.OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        //判断订单状态是否为空，如果不为空，说明订单没有支付
                        if (orderState.value() != null) {
                            //输出提示信息
                            out.collect("订单ID：" + orderState.value().split(",")[0] + "已经超过5s没有支付，请尽快支付！");
                            //清空订单状态
                            orderState.clear();
                        }

                        //判断支付状态是否为空，如果不为空，说明订单已经支付，但没有订单信息！
                        if (payState.value() != null) {
                            //输出提示信息
                            out.collect("订单ID：" + payState.value().split(",")[0] + "有异常，有支付信息没有订单信息！");
                            //清空支付状态
                            payState.clear();
                        }

                    }
                }).print();
        env.execute();
    }
}
