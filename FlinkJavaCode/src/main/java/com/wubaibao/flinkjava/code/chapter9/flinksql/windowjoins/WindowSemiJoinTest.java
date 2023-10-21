package com.wubaibao.flinkjava.code.chapter9.flinksql.windowjoins;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Flink SQL - Window Join - Semi Join
 * 案例：读取kafka中 left-topic 和 right-topic数据，形成两个表进行window semi join操作。
 */
public class WindowSemiJoinTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");


        //读取Kafka left-topic数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table left_tbl (" +
                "   id int," +
                "   name string," +
                "   age int," +
                "   dt bigint," +
                "   rowtime AS TO_TIMESTAMP_LTZ(dt,3)," +
                "   WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'left-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //读取Kafka right-topic数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table right_tbl (" +
                "   id int," +
                "   name string," +
                "   score int," +
                "   dt bigint," +
                "   rowtime AS TO_TIMESTAMP_LTZ(dt,3)," +
                "   WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'right-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //SQL 方式实现 Window Full Outer Join
        TableResult result = tableEnv.executeSql("" +
                "SELECT * FROM (" +
                "  SELECT * FROM TABLE(TUMBLE(TABLE left_tbl,DESCRIPTOR(rowtime), INTERVAL '5' SECOND))" +
                ") L WHERE EXISTS (" +
                "  SELECT * FROM ( " +
                "    SELECT * FROM TABLE(TUMBLE(TABLE right_tbl,DESCRIPTOR(rowtime), INTERVAL '5' SECOND)) " +
                "  ) R WHERE L.id = R.id AND L.window_start = R.window_start AND L.window_end = R.window_end" +
                ")" );

        //输出结果
        result.print();
    }
}
