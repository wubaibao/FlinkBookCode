package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 用户自定义Sink Connector测试类 - RedisSinkConnector
 */
public class UserDefinedSinkConnectorTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //通过自定义SocketConnector创建表
        tableEnv.executeSql("CREATE TABLE SocketSource (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(call_time,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") WITH (" +
                "  'connector' = 'socket'," +
                "  'hostname' = 'node5'," +
                "  'port' = '9999'," +
                "  'byte-delimiter' = '10'," +
                "  'format' = 'csv'" +
                ");");

        //通过自定义RedisConnector创建表
        tableEnv.executeSql("CREATE TABLE RedisSink (" +
                "   sid string," +
                "   window_start string," +
                "   window_end string," +
                "   total_dur bigint" +
                ") WITH (" +
                "  'connector' = 'redis'," +
                "  'hostname' = 'node4'," +
                "  'port' = '6379'," +
                "  'db-num' = '0'" +
                ");");

        //设置5秒的窗口计算
        Table result = tableEnv.sqlQuery("select " +
                "sid," +
                "window_start," +
                "window_end," +
                "sum(duration) as total_dur " +
                "from TABLE(" +
                "   TUMBLE(TABLE SocketSource,DESCRIPTOR(time_ltz), INTERVAL '5' SECOND)" +
                ") " +
                "group by sid,window_start,window_end");

        //打印result的表结构
        tableEnv.executeSql("desc " + result).print();
        //将结果写入到Redis中
        tableEnv.executeSql("insert into RedisSink " +
                "select sid," +
                "DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') as window_start," +
                "DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') as window_end," +
                "total_dur " +
                "from " + result);

    }
}
