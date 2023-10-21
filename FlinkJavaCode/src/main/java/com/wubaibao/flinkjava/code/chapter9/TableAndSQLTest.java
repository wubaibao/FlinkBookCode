package com.wubaibao.flinkjava.code.chapter9;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink Table和SQL 代码测试
 */
public class TableAndSQLTest {
    public static void main(String[] args) {
        //1.准备环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//使用流模式
//                .inBatchMode() //使用批模式
                .build();

        //2.创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置并行度为1
        tableEnv.getConfig().getConfiguration().setString("parallelism.default", "1");

        //3.通过Table API 创建Source表
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                // 定义表结构
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                // 每秒钟生成1条数据
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 10L)
                .build());

        //4.通过SQL DDL创建Sink Table
        tableEnv.executeSql("CREATE TABLE SinkTable (" +
                "  f0 STRING" +
                ") WITH (" +
                "  'connector' = 'print'" +
                ")");

        //5.通过Table API查询数据
        Table table1 = tableEnv.from("SourceTable");

        //6.通过SQL查询数据
        Table table2 = tableEnv.sqlQuery("select * from SourceTable");

        //7.通过Table API将查询结果写入SinkTable
        table1.executeInsert("SinkTable");

        //8.通过SQL将查询结果写入SinkTable
        tableEnv.executeSql("insert into SinkTable select * from SourceTable");

    }
}
