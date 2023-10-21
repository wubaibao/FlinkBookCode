package com.wubaibao.flinkjava.code.chapter9.tableoperator;


import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table API 查询表数据
 * 案例：读取Kafka基站日志数据，统计每个基站通话总时长。
 * 要求：过滤通话成功并且通话时长大于10的数据信息。
 */
public class QueryTableWithTableAPI {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //读取Kafka基站日志数据，通过 TableDescriptor 定义表结构
        tableEnv.createTemporaryTable("stationlog_tbl", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("sid", DataTypes.STRING())
                        .column("call_out", DataTypes.STRING())
                        .column("call_in", DataTypes.STRING())
                        .column("call_type", DataTypes.STRING())
                        .column("call_time", DataTypes.BIGINT())
                        .column("duration", DataTypes.BIGINT())
                        .build())
                .option("topic", "stationlog-topic")
                .option("properties.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
                .option("properties.group.id", "testGroup")
                .option("scan.startup.mode", "latest-offset")
                .option("format", "csv")
                .build());

        //通过Table API 获取Table对象
        Table stationLogTbl = tableEnv.from("stationlog_tbl");

       //过滤通话成功并且通话时长大于10的数据信息
        Table resultTbl = stationLogTbl
                .filter($("call_type").isEqual("success").and($("duration").isGreater(10)))
                .groupBy($("sid"))
                .select($("sid"),$("duration").sum().as("total_duration"));

        //打印输出
        TableResult execute = resultTbl.execute();
        execute.print();
    }
}
