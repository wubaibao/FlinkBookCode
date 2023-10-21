package com.wubaibao.flinkjava.code.chapter9.dsandtableintegration.tabletods;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table 转换为 DataStream
 * 使用 tableEnv.toChangelogStream(Table) 方法将 Table 转换为 DataStream
 */
public class ToChangelogStreamTest1 {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为了能看出watermark传递效果，这里设置并行度为1
        env.setParallelism(1);

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取socket中基站日志数据并转换为StationgLog类型DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = env.socketTextStream("node5", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.parseLong(split[4]), Long.parseLong(split[5]));
                });

        //设置watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<StationLog>() {
                            @Override
                            public long extractTimestamp(StationLog element, long recordTimestamp) {
                                return element.getCallTime();
                            }
                        })
        );

        //将DataStream 转换成 Table
        Table table = tableEnv.fromDataStream(
                dsWithWatermark,
                Schema.newBuilder()
                        .columnByExpression("rowtime","TO_TIMESTAMP_LTZ(callTime,3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build()
        );

        //使用Table API 对 Table进行查询
        Table resultTbl = table
                .groupBy($("sid"))
                .select($("sid"), $("duration").sum().as("totalDuration"));

        //将Table转换为DataStream
        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(resultTbl);

        //打印watermark
        rowDataStream.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect("数据："+row+"，当前watermark为：" + context.timerService().currentWatermark());
            }
        }).print();

        //执行任务
        env.execute();
    }
}
