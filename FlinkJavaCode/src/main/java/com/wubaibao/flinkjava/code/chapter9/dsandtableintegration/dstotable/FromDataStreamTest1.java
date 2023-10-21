package com.wubaibao.flinkjava.code.chapter9.dsandtableintegration.dstotable;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink DataStream 转换为 Table
 * 使用 tableEnv.fromDataStream() 方法将 DataStream 转换为 Table
 */
public class FromDataStreamTest1 {
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

        //将DataStream 转换成 Table
        Table table = tableEnv.fromDataStream(stationLogDS);

        //打印表结构
        table.printSchema();

        //打印数据结果
        TableResult tableResult = table.execute();
        tableResult.print();

    }
}
