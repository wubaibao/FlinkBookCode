package com.wubaibao.flinkjava.code.chapter9.tableoperator;

import org.apache.flink.table.api.*;

/**
 * Flink Table API 写入数据到文件系统
 * 案例：读取Kafka基站日志数据，统计每个基站通话总时长，写出到文件系统。
 * 要求：过滤通话成功并且通话时长大于10的数据信息。
 * 注意：将Table写出到文件系统，必须设置checkpoint
 */
public class TableSinkWithTableAPI {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //将Table写出文件中必须设置checkpoint,Flink SQL 中设置checkpoint的间隔
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

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

        //通过Table API 方式查询结果数据
        Table tableResult = tableEnv.from("stationlog_tbl");

        //通过 TableDescriptor 定义输出表结构
        tableEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("sid", DataTypes.STRING())
                        .column("call_out", DataTypes.STRING())
                        .column("call_in", DataTypes.STRING())
                        .column("call_type", DataTypes.STRING())
                        .column("call_time", DataTypes.BIGINT())
                        .column("duration", DataTypes.BIGINT())
                        .build())
                .option("path", "file:///D:\\data\\flink\\output")
                //设置检查生成文件的频率，每2秒检查一次，默认1分钟
                .option("sink.rolling-policy.check-interval", "2s")
                //设置文件滚动策略，每10秒生成一个文件，默认30分钟
                .option("sink.rolling-policy.rollover-interval", "10s")
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", "|")
                        .build())
                .build());

        //将结果数据写入到文件系统
        //tableResult.executeInsert("CsvSinkTable");
        tableResult.insertInto("CsvSinkTable").execute();
    }
}
