package com.wubaibao.flinkjava.code.chapter11.datastreamapi;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectPath;

/**
 * FlinkCDC 监控 MySQL 无主键表中数据
 */
public class DataStremNonPrimaryKeyTblCDCTest {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node2")      //设置MySQL hostname
                .port(3306)             //设置MySQL port
                .databaseList("db2")    //设置捕获的数据库
                .tableList("db2.tbl") //设置捕获的数据表
                .username("root")       //设置登录MySQL用户名
                .password("123456")     //设置登录MySQL密码
                .deserializer(new JsonDebeziumDeserializationSchema()) //设置序列化将SourceRecord 转换成 Json 字符串
                //设置chunk key column
                .chunkKeyColumn(new ObjectPath("db2","tbl"),"id")
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
