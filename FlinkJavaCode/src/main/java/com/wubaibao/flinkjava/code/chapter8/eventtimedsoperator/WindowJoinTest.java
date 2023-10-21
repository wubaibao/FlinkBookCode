package com.wubaibao.flinkjava.code.chapter8.eventtimedsoperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink - WindowJoin
 * 案例：读取订单流和支付流，将订单流和支付流进行关联，输出关联后的数据
 */
public class WindowJoinTest {
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

        //将订单流和支付流进行关联，并设置窗口
        DataStream<String> result = orderDSWithWatermark.join(payDSWithWatermark)
                //设置关联条件，where是订单流，equalTo是支付流
                .where(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String orderInfo) throws Exception {
                        return orderInfo.split(",")[0];
                    }
                })
                .equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String payInfo) throws Exception {
                        return payInfo.split(",")[0];
                    }
                })
                //设置窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //关联后的数据处理
                .apply(new JoinFunction<String, String, String>() {
                    @Override
                    public String join(String orderInfo, String payInfo) throws Exception {
                        return "订单信息：" + orderInfo + " - 支付信息：" + payInfo;
                    }
                });

        result.print();

        env.execute();

    }
}
