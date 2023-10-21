package com.wubaibao.flinkjava.code.chapter9.dsandtableintegration.tabletods;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Flink Table 转换为 DataStream
 * 使用 tableEnv.toDataStream(Table) 方法将 Table 转换为 DataStream
 */
public class ToDataStreamTest1 {
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

        //将DataStream 转换成 Table
        tableEnv.createTemporaryView("stationlog_tbl", stationLogDS);
        Table table = tableEnv.from("stationlog_tbl");

        //将Table转换为DataStream
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
        rowDataStream.print();

        //执行任务
        env.execute();
    }
}
