package com.wubaibao.flinkjava.code.chapter9.flinksql.overwindow;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - Over Window
 * 案例：读取Kafka基站日志数据，设置开窗函数，统计每个基站近5秒通话时长。
 */
public class OverWindowTest1 {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");


        //读取Kafka基站日志数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table stationlog_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(call_time,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'stationlog-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //SQL Over Window
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   sid,call_time," +
                "   SUM(duration) OVER (" +
                "       PARTITION BY sid " +
                "       ORDER BY time_ltz " +
                "       RANGE BETWEEN INTERVAL '5' SECOND PRECEDING AND CURRENT ROW" +
                "   ) as sum_dur " +
                "FROM stationlog_tbl");


        //打印结果
        result.execute().print();
    }
}
