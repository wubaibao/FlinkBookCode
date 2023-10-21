package com.wubaibao.flinkjava.code.chapter9.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink 实时写入 Hive
 * 案例：读取Kafka中基站日志数据，实时写入Hive分区表中
 * hive分区表以年月日时分秒分区
 */
public class FlinkRTWriteHive {
    public static void main(String[] args) {

        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置checkpoint
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //SQL 方式创建 Hive Catalog
        tableEnv.executeSql("CREATE CATALOG myhive WITH (" +
                "  'type'='hive'," +//指定Catalog类型为hive
                "  'default-database'='default'," +//指定默认数据库
                "  'hive-conf-dir'='D:\\idea_space\\MyFlinkCode\\hiveconf'" +//指定Hive配置文件目录，Flink读取Hive元数据信息需要
                ")");

        //将 HiveCatalog 设置为当前会话的 catalog
        tableEnv.useCatalog("myhive");

        //设置Hive方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //创建Hive分区表
        tableEnv.executeSql("" +
                "create table if not exists rt_hive_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") partitioned by (dt string,hr string,mm string,ss string) " +
                "stored as parquet tblproperties (" +
                " 'partition.time-extractor.timestamp-pattern'='$dt $hr:$mm:$ss'," +
                " 'sink.partition-commit.trigger'='partition-time'," +
                " 'sink.partition-commit.delay'='1 s'," +
                " 'sink.partition-commit.watermark-time-zone'='Asia/Shanghai'," +
                " 'sink.partition-commit.policy.kind'='metastore,success-file'" +
                ")"
        );

        //设置 Flink Default方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        //读取Kafka数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table if not exists rt_kafka_tbl (" +
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

        //将 Kafka 数据写入 Hive 分区表
        tableEnv.executeSql("" +
                "insert into rt_hive_tbl " +
                "select sid,call_out,call_in,call_type,call_time,duration," +
                "   DATE_FORMAT(rowtime, 'yyyy-MM-dd') dt," +
                "   DATE_FORMAT(rowtime, 'HH') hr, " +
                "   DATE_FORMAT(rowtime, 'mm') mm, " +
                "   DATE_FORMAT(rowtime, 'ss') ss " +
                "from rt_kafka_tbl"
        );

    }
}
