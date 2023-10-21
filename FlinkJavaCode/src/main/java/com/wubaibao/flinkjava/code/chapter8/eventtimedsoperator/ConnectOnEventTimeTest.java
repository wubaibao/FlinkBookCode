package com.wubaibao.flinkjava.code.chapter8.eventtimedsoperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 案例：读取socket中数据流形成两个流，进行Connect关联观察水位线。
 */
public class ConnectOnEventTimeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //方便测试，并行度设置为1
        env.setParallelism(1);
        /**
         * 读取socket中数据流形成A流，并对A流设置watermark
         * 格式：001,181,182,busy,1000,10
         */
        SingleOutputStreamOperator<String> ADS = env.socketTextStream("node5", 8888)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        String[] arr = s.split(",");
                        //返回拼接字符串
                        return arr[0].trim() + "," +
                                arr[1].trim() + "," +
                                arr[2].trim() + "," +
                                arr[3].trim() + "," +
                                arr[4].trim() + "," +
                                arr[5].trim();
                    }
                });

        //设置水位线
        SingleOutputStreamOperator<String> AdsWithWatermark = ADS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((str, timestamp) -> Long.valueOf(str.split(",")[4]))
        );

        /**
         * 读取socket中数据流形成B流,并对B流设置watermark
         * 格式：1,3000
         */
        SingleOutputStreamOperator<String> BDS = env.socketTextStream("node5", 9999);

        //设置水位线
        SingleOutputStreamOperator<String> BdsWithWatermark = BDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((str, timestamp) -> Long.valueOf(str.split(",")[1]))
        );

        //两流进行connect操作
        AdsWithWatermark.connect(BdsWithWatermark)
                .process(new CoProcessFunction<String, String, String>() {
                    @Override
                    public void processElement1(String value,
                                                CoProcessFunction<String, String, String>.Context ctx,
                                                Collector<String> out) throws Exception {
                        out.collect("A流数据：" + value + ",当前watermark：" + ctx.timerService().currentWatermark());

                    }

                    @Override
                    public void processElement2(String value,
                                                CoProcessFunction<String, String, String>.Context ctx,
                                                Collector<String> out) throws Exception {
                        out.collect("B流数据：" + value + ",当前watermark：" + ctx.timerService().currentWatermark());

                    }
                }).print();

        env.execute();
    }
}
