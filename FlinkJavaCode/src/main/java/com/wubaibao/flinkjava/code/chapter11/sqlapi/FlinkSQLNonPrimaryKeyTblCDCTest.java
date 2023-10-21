package com.wubaibao.flinkjava.code.chapter11.sqlapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * FlinkSQL FlinkCDC 监控 MySQL 无主键表中数据
 */
public class FlinkSQLNonPrimaryKeyTblCDCTest {
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
                " 'database-name' = 'db2'," +
                " 'table-name' = 'tbl'," +
                " 'scan.incremental.snapshot.chunk.key-column' = 'id' " +
                ")");

        tableEnv.executeSql("select * from mysql_binlog").print();
    }
}
