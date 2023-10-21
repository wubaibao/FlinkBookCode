package com.wubaibao.flinkjava.code.chapter11.datastreamapi;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * MySQL CDC Exactly-once 语义测试
 */
public class ExactlyOnceTest {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node2")      //设置MySQL hostname
                .port(3306)             //设置MySQL port
                .databaseList("db3")    //设置捕获的数据库
                .tableList("db3.test") //设置捕获的数据表
                .username("root")       //设置登录MySQL用户名
                .password("123456")     //设置登录MySQL密码
                .deserializer(new JsonDebeziumDeserializationSchema()) //设置序列化将SourceRecord 转换成 Json 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(5000);

        //设置checkpoint Storage存储为hdfs
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/flinkcdc-cks");

        //设置checkpoint清理策略为RETAIN_ON_CANCELLATION,默认也是不清空
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySQL Source")
                .setParallelism(4)
                .print();

        env.execute();

    }
}

