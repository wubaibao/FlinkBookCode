package com.wubaibao.flinkjava.code.chapter10.sqlcep;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL CEP - DEFINE 子句中使用聚合函数
 */
public class DefineWithAggTest {
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
                "   rowtime AS TO_TIMESTAMP_LTZ(call_time,3)," +
                "   WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'stationlog-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //SQL CEP - 输出连续2次通话失败的基站信息
        tableEnv.executeSql("" +
                "select " +
                "   T.sid,T.start_dt,T.end_dt,T.avgDuration,T.dt " +
                "from stationlog_tbl " +
                " MATCH_RECOGNIZE ( " +
                "   PARTITION BY sid " +
                "   ORDER BY rowtime " +
                "   MEASURES " +
                "       FIRST(A.rowtime) AS start_dt," +
                "       LAST(A.rowtime) AS end_dt," +
                "       AVG(A.duration) AS avgDuration," +
                "       B.rowtime AS dt " +
                "   ONE ROW PER MATCH " +
                "   AFTER MATCH SKIP PAST LAST ROW " +//在当前匹配的最后一行之后的下一行继续模式匹配
                "   PATTERN (A+ B) " +
                "   DEFINE " +
                "       A AS AVG(A.duration) < 10" +
                ") T" +
                "").print();
    }
}
