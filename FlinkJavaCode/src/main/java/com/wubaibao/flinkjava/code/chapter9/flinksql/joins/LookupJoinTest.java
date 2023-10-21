package com.wubaibao.flinkjava.code.chapter9.flinksql.joins;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - Join操作 - Lookup Join
 * 案例：读取MySQL中数据形成维表与读取Kafka中数据形成流表，通过Lookup Join查询维表中数据。
 */
public class LookupJoinTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");


        //读取Kafka 浏览商品数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table visit_tbl (" +
                "   product_id string," +
                "   visit_time bigint," +
                "   proc_time AS PROCTIME()," +
                "   rowtime AS TO_TIMESTAMP_LTZ(visit_time,3)," +
                "   WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'visit-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //读取 MySQL 商品信息，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table product_tbl (" +
                "   product_id string," +
                "   product_name string," +
                "   price double" +
                ") with (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://node2:3306/mydb'," +
                "   'table-name' = 'product_tbl'," +
                "   'username' = 'root'," +
                "   'password' = '123456'" +
                ")");

        //SQL 方式实现 Temporal Join
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "  l.product_id,l.visit_time,l.rowtime,r.product_name,r.price " +
                "from visit_tbl l " +
                "JOIN product_tbl FOR SYSTEM_TIME AS OF l.proc_time r " +
                "ON l.product_id = r.product_id"
        );

        //打印结果
        result.execute().print();
    }
}
