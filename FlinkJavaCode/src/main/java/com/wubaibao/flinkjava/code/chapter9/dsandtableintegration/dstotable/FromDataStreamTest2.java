package com.wubaibao.flinkjava.code.chapter9.dsandtableintegration.dstotable;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink DataStream 转换为 Table
 * 使用 tableEnv.fromDataStream() 方法将 DataStream 转换为 Table
 */
public class FromDataStreamTest2 {
    public static void main(String[] args) {
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

        Table table = tableEnv.fromDataStream(stationLogDS,
                Schema.newBuilder()
                        //指定字段类型，第一个参数是指定名，第二个参数是指定类型
//                        .column("sid", DataTypes.STRING())
//                        .column("callOut", DataTypes.STRING())
//                        .column("callIn", DataTypes.STRING())
//                        .column("callType", DataTypes.STRING())
//                        .column("callTime", DataTypes.BIGINT())
//                        .column("duration", DataTypes.BIGINT())
                        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是指定为当前处理时间
                        .columnByExpression("proc_time", "PROCTIME()")
                        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build());

        //打印表结构
        table.printSchema();

        //打印数据结果
        TableResult tableResult = table.execute();
        tableResult.print();
    }
}
