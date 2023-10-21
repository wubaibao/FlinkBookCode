package com.wubaibao.flinkjava.code.chapter9.tableapi.windows;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Flink Table API  - Tumbling Window
 * 案例:读取socket基站日志数据，每隔5s统计窗口通话时长。
 */
public class WindowTest {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //读取socket中基站日志数据并转换为StationgLog类型DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = env.socketTextStream("node5", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });

        Table table = tableEnv.fromDataStream(stationLogDS,
                Schema.newBuilder()
                        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build());

        //基于时间的滑动窗口
        Table result = table.window(
                 Tumble.over(lit(5).seconds())
                         .on($("rowtime"))
                         .as("w")
                )
                .groupBy($("sid"), $("w"))
                .select(
                        $("sid"),
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("duration").sum().as("total_duration")
                );

        //打印结果
        result.execute().print();

    }
}
