package com.wubaibao.flinkjava.code.chapter9.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 *  Flink Connector 之 HBase Connector
 *  案例：通过 Kafka Connector读取 Kafka 变更日志数据并将结果写入到 HBase Conector创建的表中
 */
public class HBaseConnectorTest2 {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //读取Kafka 数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table KafkaSourceTbl (" +
                "   id int," +
                "   name string," +
                "   age int," +
                "   score double," +
                "   PRIMARY KEY(id) NOT ENFORCED" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 't1'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'debezium-json'" +
                ")");

        //HBase Sink ，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table HBaseSinkTbl (" +
                "   rk string," +
                "   family1 ROW<id int, name string,age int>," +
                "   family2 ROW<score double>," +
                "   PRIMARY KEY (rk) NOT ENFORCED " +
                ") with (" +
                "   'connector' = 'hbase-2.2'," +
                "   'table-name' = 't2'," +
                "   'zookeeper.quorum' = 'node3:2181,node4:2181,node5:2181'" +
                ")"
        );

        //SQL方式将数据写入sink表中
        tableEnv.executeSql("" +
                "insert into HBaseSinkTbl " +
                "select " +
                "   cast(id as string) as rk," +
                "   ROW(id,name,age) as family1," +
                "   ROW(score) as family2 " +
                "from KafkaSourceTbl"
        );

    }
}
