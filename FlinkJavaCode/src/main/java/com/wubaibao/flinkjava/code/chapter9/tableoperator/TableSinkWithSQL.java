package com.wubaibao.flinkjava.code.chapter9.tableoperator;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL 写入数据到文件系统
 * 案例：读取Kafka基站日志数据，统计每个基站通话总时长，写出到文件系统。
 * 要求：过滤通话成功并且通话时长大于10的数据信息。
 * 注意：将Table写出到文件系统，必须设置checkpoint
 */
public class TableSinkWithSQL {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //将Table写出文件中必须设置checkpoint,Flink SQL 中设置checkpoint的间隔
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        //读取Kafka基站日志数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table stationlog_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'stationlog-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //通过SQL DDL方式定义文件系统表
        tableEnv.executeSql("" +
                "create table CsvSinkTable (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") with (" +
                "   'connector' = 'filesystem'," +
                "   'path' = 'file:///D:/data/flinkdata/result'," +
                "   'sink.rolling-policy.check-interval' = '2s'," +
                "   'sink.rolling-policy.rollover-interval' = '10s'," +
                "   'format' = 'csv'," +
                "   'csv.field-delimiter' = '|'" +
                ")");

        //SQL方式将数据写入文件系统
        tableEnv.executeSql("" +
                "insert into CsvSinkTable " +
                "select sid,call_out,call_in,call_type,call_time,duration " +
                "from stationlog_tbl ");
    }
}
