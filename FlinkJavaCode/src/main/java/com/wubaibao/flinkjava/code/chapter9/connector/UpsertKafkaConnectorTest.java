package com.wubaibao.flinkjava.code.chapter9.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 *  Flink Connector 之 UpsertKafka Connector
 *  案例：通过Table Connector读取Kafka数据并聚合后将结果写出到Kafka
 */
public class UpsertKafkaConnectorTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //Kafka Source，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table KafkaSourceTbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime AS TO_TIMESTAMP_LTZ(call_time, 3)," +
                "   WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 't1'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")"
        );

        //定义 Kafka Sink，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table KafkaSinkTbl (" +
                "   sid string," +
                "   sum_duration bigint," +
                "   PRIMARY KEY (sid) NOT ENFORCED" +
                ") with (" +
                "   'connector' = 'upsert-kafka'," +
                "   'topic' = 't3'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'key.format' = 'csv'," +
                "   'value.format' = 'csv'" +
                ")");


        //SQL方式将数据写入文件系统
        tableEnv.executeSql("" +
                "insert into KafkaSinkTbl " +
                "select sid,sum(duration) as sum_duration " +
                "from KafkaSourceTbl " +
                "group by sid");
    }
}
