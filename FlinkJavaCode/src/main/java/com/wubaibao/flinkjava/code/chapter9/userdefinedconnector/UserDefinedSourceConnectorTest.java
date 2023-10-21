package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 用户自定义Soruce Connector测试类 - SocketSource
 * 注意Socket Source只能是单线程方式运行
 */
public class UserDefinedSourceConnectorTest {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

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

        //打印表结构
        tableEnv.executeSql("desc SocketSource").print();

        //设置5秒的窗口计算
        TableResult result = tableEnv.executeSql("select " +
                "sid," +
                "window_start," +
                "window_end," +
                "sum(duration) as total_dur " +
                "from TABLE(" +
                "   TUMBLE(TABLE SocketSource,DESCRIPTOR(time_ltz), INTERVAL '5' SECOND)" +
                ") " +
                "group by sid,window_start,window_end");

        result.print();

    }
}
