package com.wubaibao.flinkjava.code.chapter9.flinksql.joins;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - Join操作 - Interval Join
 * 案例：从Kafka中读取用户登录流和广告点击流数据，通过IntervalJoin分析用户点击广告的行为
 */
public class IntervalJoinTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");


        //读取Kafka 登录数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table login_tbl (" +
                "   user_id string," +
                "   login_time bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(login_time,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'login-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //读取Kafka 点击广告信息，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table click_tbl (" +
                "   user_id string," +
                "   product_id string," +
                "   dt bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(dt,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'click-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //SQL 方式实现Interval Join
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   l.user_id," +
                "   l.login_time," +
                "   c.product_id," +
                "   c.dt " +
                "from login_tbl as l " +
                "join click_tbl as c " +
                "on l.user_id = c.user_id " +
                "and l.time_ltz between c.time_ltz - INTERVAL '2' SECOND and c.time_ltz + INTERVAL '2' SECOND");

        //打印结果
        result.execute().print();
    }
}
