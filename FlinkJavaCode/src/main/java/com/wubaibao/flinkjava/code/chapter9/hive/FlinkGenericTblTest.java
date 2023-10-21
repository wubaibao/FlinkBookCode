package com.wubaibao.flinkjava.code.chapter9.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL 创建表使用Hive MetaStrore进行元数据管理
 */
public class FlinkGenericTblTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //SQL 方式创建 Hive Catalog
        tableEnv.executeSql("CREATE CATALOG myhive WITH (" +
                "  'type'='hive'," +//指定Catalog类型为hive
                "  'default-database'='default'," +//指定默认数据库
                "  'hive-conf-dir'='D:\\idea_space\\MyFlinkCode\\hiveconf'" +//指定Hive配置文件目录，Flink读取Hive元数据信息需要
                ")");

        //将 HiveCatalog 设置为当前会话的 catalog
        tableEnv.useCatalog("myhive");

        //读取Kafka数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table flink_kafka_tbl (" +
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
                "   'topic' = 'stationlog-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")"
        );

        //通过SQL 查询表中数据
        tableEnv.executeSql("select * from flink_kafka_tbl").print();

    }
}
