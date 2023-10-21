package com.wubaibao.flinkjava.code.chapter10.sqlcep;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL CEP - 贪婪模式和勉强模式
 * 案例：Flink SQL读取Kafka基站日志数据，通过SQL CEP找出符合如下模式的事件
 * （通话时长大于10的一个事件、多个通话时长小于15的事件、1个通话时长大于12的事件）
 */
public class GreedyTest {
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

        //SQL CEP - 匹配符合(1个通话时长大于10的事件、多个通话时长小于15的事件、1个通话时长大于12的事件)模式序列
        tableEnv.executeSql("" +
                "select " +
                "   T.sid,T.dt,T.duration " +
                "from stationlog_tbl " +
                " MATCH_RECOGNIZE ( " +
                "   PARTITION BY sid " +
                "   ORDER BY rowtime " +
                "   MEASURES " +
                "       C.rowtime AS dt, " +
                "       C.duration AS duration " +
                "   ONE ROW PER MATCH " +
                "   AFTER MATCH SKIP PAST LAST ROW " +
                "   PATTERN (A B*? C) " +
                "   DEFINE " +
                "       A as A.duration > 10," +
                "       B as B.duration < 15," +
                "       C as C.duration > 12" +
                ") T" +
                "").print();
    }
}
