package com.wubaibao.flinkjava.code.chapter10.sqlcep;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL CEP - WithIn 时间限制测试
 * 案例：Flink SQL读取Kafka基站日志数据，通过SQL CEP方式匹配基站通话失败后如果在5秒内没有通话成功信息就输出告警信息
 */
public class WithInTest {
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

        //SQL CEP - 基站通话失败后如果在5秒内没有通话成功信息就输出告警信息
        tableEnv.executeSql("" +
                "select " +
                "   T.sid,T.fail_dt,T.success_dt " +
                "from stationlog_tbl " +
                " MATCH_RECOGNIZE ( " +
                "   PARTITION BY sid " +
                "   ORDER BY rowtime " +
                "   MEASURES " +
                "       FIRST(A.rowtime) AS fail_dt," +
                "       B.rowtime AS success_dt " +
                "   ONE ROW PER MATCH " +
//                "   AFTER MATCH SKIP TO LAST B " +
                "   PATTERN (A+ B) WITHIN INTERVAL '5' SECOND " +
                "   DEFINE " +
                "       A as A.call_type = 'fail'," +
                "       B as B.call_type = 'success'" +
                ") T" +
                "").print();
    }
}
