package com.wubaibao.flinkjava.code.chapter9.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Connector之Filesystem Connector
 * 案例：通过Table Connector读取文件系统数据并写出到文件系统
 */
public class FilesystemConnectorTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //将Table写出文件中必须设置checkpoint,Flink SQL 中设置checkpoint的间隔
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        //读取文件系统日志数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table CsvSourceTable (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime AS TO_TIMESTAMP_LTZ(call_time,3)," +
                "   WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'filesystem'," +
                "   'path' = 'file:///D:/data/flinksource'," +
                "   'format' = 'csv'," +
                "   'csv.field-delimiter' = ','" +
                ")");

        //通过SQL DDL方式定义文件系统表
        tableEnv.executeSql("" +
                "create table CsvSinkTable (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime TIMESTAMP_LTZ(3)" +
                ") with (" +
                "   'connector' = 'filesystem'," +
                "   'path' = 'file:///D:/data/flinksink'," +
                "   'sink.rolling-policy.check-interval' = '2s'," +
                "   'sink.rolling-policy.rollover-interval' = '10s'," +
                "   'format' = 'csv'," +
                "   'csv.field-delimiter' = '|'" +
                ")");

        //SQL方式将数据写入文件系统
        tableEnv.executeSql("" +
                "insert into CsvSinkTable " +
                "select sid,call_out,call_in,call_type,call_time,duration,rowtime " +
                "from CsvSourceTable ");

    }
}
