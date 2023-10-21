package com.wubaibao.flinkjava.code.chapter9.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 *  Flink Connector 之 JDBC Connector
 *  案例：通过JDBC Connector读取Kafka变更日志数据并将结果写入到JDBC Conector创建的表中
 *   Kafka 数据为变更日志数据
 */
public class JDBCConnectorTest2 {
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
                "   PRIMARY KEY(id) NOT ENFORCED" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 't1'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'debezium-json'" +
                ")");

        //MySQL Sink ，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table MysqlSinkTbl (" +
                "   id int," +
                "   name string," +
                "   age int," +
                "   PRIMARY KEY(id) NOT ENFORCED " +
                ") with (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://node2:3306/mydb?useSSL=false'," +
                "   'table-name' = 't2'," +
                "   'username' = 'root'," +
                "   'password' = '123456'" +
                ")"
        );

        //SQL方式将数据写入sink表中
        tableEnv.executeSql("" +
                "insert into MysqlSinkTbl " +
                "select id,name,age " +
                "from KafkaSourceTbl"
        );

    }

}
