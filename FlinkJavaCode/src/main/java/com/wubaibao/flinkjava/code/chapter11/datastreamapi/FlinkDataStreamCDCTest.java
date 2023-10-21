package com.wubaibao.flinkjava.code.chapter11.datastreamapi;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FlinkCDC 监控 MySQL 中数据 ，监控指定库和表，从头开始读取binlog
 */
public class FlinkDataStreamCDCTest {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node2")      //设置MySQL hostname
                .port(3306)             //设置MySQL port
                .databaseList("db1")    //设置捕获的数据库
                .tableList("db1.tbl1,db1.tbl2") //设置捕获的数据表
                .username("root")       //设置登录MySQL用户名
                .password("123456")     //设置登录MySQL密码
                .deserializer(new JsonDebeziumDeserializationSchema()) //设置序列化将SourceRecord 转换成 Json 字符串
                .startupOptions(StartupOptions.initial())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(5000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySQL Source")
                .setParallelism(4)
                .print();

        env.execute();

    }
}
