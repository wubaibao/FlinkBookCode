package com.wubaibao.flinkjava.code.chapter9.flinksql.joins;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - Join操作 - Full Outer Join
 * 案例：读取Kafka 中订单数据及商品数据，进行关联输出订单详细信息。
 */
public class FullOuterJoinTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");


        //读取Kafka 订单数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table order_tbl (" +
                "   order_id string," +
                "   product_id string," +
                "   order_amount double," +
                "   order_time bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(order_time,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'order-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //读取Kafka 商品信息，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table product_tbl (" +
                "   product_id string," +
                "   product_name string," +
                "   dt bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(dt,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'product-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //SQL Full Outer Join
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   a.order_id,b.product_name,a.order_amount,a.order_time " +
                "from " +
                "   order_tbl a " +
                "full outer join product_tbl b " +
                "on a.product_id = b.product_id");

        //打印结果
        result.execute().print();
    }
}
