package com.wubaibao.flinkjava.code.chapter9.tableoperator;

import org.apache.flink.table.api.*;

/**
 * Flink SQL 查询表数据
 * 案例：读取Kafka基站日志数据，统计每个基站通话总时长。
 * 要求：过滤通话成功并且通话时长大于10的数据信息。
 */
public class QueryTableWithSQL {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

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

        //通过SQL统计过滤通话成功并且通话时长大于10的数据信息
        Table resultTbl = tableEnv.sqlQuery("" +
                "select sid,sum(duration) as total_duration " +
                "from stationlog_tbl " +
                "where call_type='success' and duration>10 " +
                "group by sid");

        //打印结果
        resultTbl.execute().print();
    }
}
