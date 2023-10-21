package com.wubaibao.flinkjava.code.chapter8.eventtimedsoperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink - IntervalJoin
 * 案例：读取用户登录流和广告点击流，通过Interval Join分析用户点击广告的行为
 */
public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //方便测试，并行度设置为1
        env.setParallelism(1);
        /**
         * 读取socket中用户登录流，并对用户登录流设置watermark
         * 用户登录流数据格式:用户ID,登录时间
         * user_1,1000
         */
        SingleOutputStreamOperator<String> loginDS = env.socketTextStream("node5", 8888);
        //设置水位线
        SingleOutputStreamOperator<String> loginDSWithWatermark = loginDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((loginInfo, timestamp) -> Long.valueOf(loginInfo.split(",")[1]))
        );

        /**
         * 读取socket中广告点击流，并对广告点击流设置watermark
         *  广告点击流数据格式:用户ID,广告ID,点击时间
         *  user_1,product_1,1000
         */
        SingleOutputStreamOperator<String> clickDS = env.socketTextStream("node5", 9999);
        //设置水位线
        SingleOutputStreamOperator<String> clickDSWithWatermark = clickDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((clickInfo, timestamp) -> Long.valueOf(clickInfo.split(",")[2]))
        );

        //Interval Join
        loginDSWithWatermark.keyBy(loginInfo -> loginInfo.split(",")[0])
                .intervalJoin(clickDSWithWatermark.keyBy(clickInfo -> clickInfo.split(",")[0]))
                //设置时间范围
                .between(Time.seconds(-2), Time.seconds(2))
                //设置处理函数
                .process(new ProcessJoinFunction<String, String, String>() {
                    @Override
                    public void processElement(String left, String right, ProcessJoinFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        //获取用户ID
                        String userId = left.split(",")[0];
                        out.collect("用户ID为：" + userId + "的用户点击了广告：" + right);

                    }
                })
                .print();

        env.execute();
    }
}
