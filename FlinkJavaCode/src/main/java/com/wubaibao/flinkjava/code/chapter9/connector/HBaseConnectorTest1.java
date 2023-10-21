package com.wubaibao.flinkjava.code.chapter9.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 *  Flink Connector 之 HBase Connector
 *  案例：通过 HBase Connector读取 HBase 数据并将结果写入到 HBase Conector创建的表中
 */
public class HBaseConnectorTest1 {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //HBase Source，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table HBaseSourceTbl (" +
                "   rk string," +
                "   cf1 ROW<id string, name string,age string>," +
                "   cf2 ROW<score string>," +
                "   PRIMARY KEY(rk) NOT ENFORCED " +
                ") with (" +
                "   'connector' = 'hbase-2.2'," +
                "   'table-name' = 't1'," +
                "   'zookeeper.quorum' = 'node3:2181,node4:2181,node5:2181'" +
                ")"
        );

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
        Table table = tableEnv.sqlQuery("select " +
                "rk," +
                "CAST(cf1.id AS INT) as id ," +
                "cf1.name as name," +
                "CAST(cf1.age AS INT) as age ," +
                "CAST(cf2.score AS DOUBLE) as score " +
                "from HBaseSourceTbl");

        tableEnv.executeSql("" +
                "insert into HBaseSinkTbl " +
                "select rk, ROW(id,name,age) as family1 ,ROW(score) as family2 from " + table
        );

    }
}
