package com.wubaibao.flinkjava.code.chapter9.udf;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink Table和SQL - 自定义表聚合函数
 * 案例：读取Kafka数据形成表，通过自定义表聚合函数获取每个基站通话时长top2
 */
public class TableAggregateFunctionTest {
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

        //注册自定义表聚合函数
        tableEnv.createTemporarySystemFunction("my_top2", Top2DurationTableUDAF.class);

        //Table API 方式调用自定义表聚合函数
        Table result1 = tableEnv.from("stationlog_tbl")
                .groupBy($("sid"))
                .flatAggregate(call("my_top2", $("duration")).as("top2_duration","rank"))
                .select($("sid"), $("top2_duration"), $("rank"));

        result1.execute().print();

    }
}
