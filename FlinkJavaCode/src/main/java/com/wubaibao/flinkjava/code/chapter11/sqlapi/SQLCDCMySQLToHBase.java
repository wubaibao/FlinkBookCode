package com.wubaibao.flinkjava.code.chapter11.sqlapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 通过Flink MySQL CDC 将MySQL数据同步到Hbase中
 * SQL API 实现
 */
public class SQLCDCMySQLToHBase {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置checkpoint
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        tableEnv.executeSql("" +
                "CREATE TABLE mysql_binlog (" +
                " id INT," +
                " name STRING," +
                " age INT," +
                " PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'node2'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'db1'," +
                " 'table-name' = 'tbl1'" +
                ")");

        //HBase Sink ，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table HBaseSinkTbl (" +
                "   rk string," +
                "   cf1 ROW<id string, name string,age string>," +
                "   PRIMARY KEY (rk) NOT ENFORCED " +
                ") with (" +
                "   'connector' = 'hbase-2.2'," +
                "   'table-name' = 'tbl_cdc'," +
                "   'zookeeper.quorum' = 'node3:2181,node4:2181,node5:2181'" +

                ")"
        );

        //sql将MySQL数据写入到hbase表中
        tableEnv.executeSql("" +
                "insert into HBaseSinkTbl " +
                "select CAST(id AS STRING) AS rk, ROW(CAST(id AS STRING),name,CAST(age AS STRING)) as cf1 from mysql_binlog"
        );


    }
}
