package com.wubaibao.flinkjava.code.chapter9.time;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table API和SQL编程中指定 Event Time
 * 案例:读取Kafka中基站日志数据，每隔5秒进行数据条数统计
 */
public class EventTimeTest1 {
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

        //通过SQL每隔5秒统计一次数据条数
        Table resultTbl = tableEnv.sqlQuery("" +
                "select " +
                "TUMBLE_START(time_ltz,INTERVAL '5' SECOND) as window_start," +
                "TUMBLE_END(time_ltz,INTERVAL '5' SECOND) as window_end," +
                "count(sid) as cnt " +
                "from stationlog_tbl " +
                "group by TUMBLE(time_ltz,INTERVAL '5' SECOND)");

        //打印结果
        resultTbl.execute().print();
    }
}
