package com.wubaibao.flinkjava.code.chapter9.tableapi.overwindow;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindowedTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink Table API - 基于时间间隔的开窗函数
 * 案例：读取基站日志数据，设置时间间隔Over开窗函数，统计最近2秒每个基站的通话时长
 */
public class OverWindowBaseTime {
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

        //设置基于时间间隔的Over开窗函数
        OverWindowedTable window = table.window(
                Over.partitionBy($("sid"))
                        .orderBy($("rowtime"))
                        .preceding(lit(2).second())
                        .following(CURRENT_RANGE)
                        .as("w")
        );

        Table result = window.select(
                $("sid"),
                $("duration"),
                $("callTime"),
//                $("rowtime"),
                $("duration").sum().over($("w")).as("sum_duration"));

        //输出结果
        result.execute().print();

    }
}
