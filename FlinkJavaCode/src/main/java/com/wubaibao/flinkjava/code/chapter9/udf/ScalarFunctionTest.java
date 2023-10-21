package com.wubaibao.flinkjava.code.chapter9.udf;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table和SQL - 自定义标量函数
 * 案例：读取 Kafka 中基站日志数据，定义标量函数，将多个字段拼接为一个字符串输出
 */
public class ScalarFunctionTest {
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

        //注册自定义标量函数
        tableEnv.createTemporarySystemFunction("my_concat", ConcatStringUDF.class);

        //Table API 方式调用自定义标量函数
        /*Table result1 = tableEnv.from("stationlog_tbl")
                .select($("sid"),
                        call("my_concat",
                                $("call_out"),
                                $("call_in"),
                                $("call_type"),
                                $("duration")).as("call_info")
                );
        result1.execute().print();*/

        //SQL 方式调用自定义标量函数
        Table result2 = tableEnv.sqlQuery("" +
                "SELECT sid,my_concat(call_out,call_in,call_type,duration) as call_info " +
                "FROM stationlog_tbl");
        result2.execute().print();
    }
}
