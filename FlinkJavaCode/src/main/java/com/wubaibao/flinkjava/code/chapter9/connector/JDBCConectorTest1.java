package com.wubaibao.flinkjava.code.chapter9.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 *  Flink Connector 之 JDBC Connector
 *  案例：通过JDBC Connector读取MySQL数据并将结果写入到JDBC Conector创建的表中
 */
public class JDBCConectorTest1 {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //MySQL Source，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table MysqlSourceTbl (" +
                "   id int," +
                "   name string," +
                "   age int" +
                ") with (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://node2:3306/mydb?useSSL=false'," +
                "   'table-name' = 't1'," +
                "   'username' = 'root'," +
                "   'password' = '123456'" +
                ")"
        );

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
                "from MysqlSourceTbl"
        );
    }
}
