package com.wubaibao.flinkjava.code.chapter9.dsandtableintegration.tabletods;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table 转换为 DataStream
 * 使用 tableEnv.toChangelogStream(Table,Schema) 方法将 Table 转换为 DataStream
 */
public class ToChangelogStreamTest2 {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(
                resultTbl,
                //执行查询的列及类型
                Schema.newBuilder()
                        .column("sid", DataTypes.STRING())
                        .column("totalDuration", DataTypes.BIGINT())
                        .build(),
                ChangelogMode.upsert()
        );

        rowDataStream.print();

        //执行任务
        env.execute();
    }
}
