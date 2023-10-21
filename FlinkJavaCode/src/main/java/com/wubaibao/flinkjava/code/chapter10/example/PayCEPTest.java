package com.wubaibao.flinkjava.code.chapter10.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP 实现订单支付超时监控
 * 案例：读取基站用户订单数据，实时监控用户订单是否支付超时，
 * 如果在20秒内没有支付则输出告警信息，
 * 如果在20秒内支付了则输出支付信息。
 */
public class PayCEPTest {
    public static void main(String[] args) throws Exception {
        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.定义事件流
        SingleOutputStreamOperator<OrderInfo> ds = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, OrderInfo>() {
                    @Override
                    public OrderInfo map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new OrderInfo(arr[0], Double.valueOf(arr[1]), Long.valueOf(arr[2]), arr[3]);
                    }
                });

        //设置watermark并设置自动推进watermark
        SingleOutputStreamOperator<OrderInfo> dsWithWatermark = ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getOrderTime();
                            }
                        }).withIdleness(Duration.ofSeconds(5))
        );

        //获取每个用户的登录信息
        KeyedStream<OrderInfo, String> keyedStream = dsWithWatermark.keyBy(new KeySelector<OrderInfo, String>() {
            @Override
            public String getKey(OrderInfo value) throws Exception {
                return value.getOrderId();
            }
        });

        //2.定义匹配规则。
        Pattern<OrderInfo, OrderInfo> pattern = Pattern.<OrderInfo>begin("first")
                .where(new SimpleCondition<OrderInfo>() {
                    @Override
                    public boolean filter(OrderInfo value) throws Exception {
                        return value.getPayState().equals("create");
                    }
                }).followedBy("second").where(new SimpleCondition<OrderInfo>() {
                    @Override
                    public boolean filter(OrderInfo value) throws Exception {
                        return value.getPayState().equals("pay");
                    }
                }).within(Time.seconds(20));

        //定义outputTag
        OutputTag<String> outputTag = new OutputTag<String>("pay-timeout"){};

        //3.将匹配规则应用到数据流上
        PatternStream<OrderInfo> patternStream = CEP.pattern(keyedStream, pattern);

        //4.获取符合规则的数据
        SingleOutputStreamOperator<String> result = patternStream.process(new MyPatternProcessFunction(outputTag));

        //打印结果
        result.print("订单支付：");

        //5.获取超时数据
        result.getSideOutput(outputTag).print("超时数据：");

        env.execute();
    }
}

class MyPatternProcessFunction extends PatternProcessFunction<OrderInfo,String>
        implements TimedOutPartialMatchHandler<OrderInfo>{
    //定义outputTag
    private OutputTag<String> outputTag;

    //创建MyPatternProcessFunction构造器用于接收outputTag
    public MyPatternProcessFunction(OutputTag<String> outputTag) {
        this.outputTag = outputTag;
    }

    //处理匹配到的数据
    @Override
    public void processMatch(Map<String, List<OrderInfo>> match,
                             Context ctx,
                             Collector<String> out) throws Exception {
        List<OrderInfo> firstPatternInfo = match.get("first");

        //获取订单
        String uid = firstPatternInfo.get(0).getOrderId();

        //输出
        out.collect("订单" + uid + "支付成功,待发货");
    }

    //处理超时的数据
    @Override
    public void processTimedOutMatch(Map<String, List<OrderInfo>> match,
                                     Context ctx) throws Exception {
        List<OrderInfo> firstPatternInfo = match.get("first");
        ctx.output(outputTag,"订单" + firstPatternInfo.get(0).getOrderId() + "支付超时");

    }
}
