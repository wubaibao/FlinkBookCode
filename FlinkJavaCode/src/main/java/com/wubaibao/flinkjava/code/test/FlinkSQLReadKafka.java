package com.wubaibao.flinkjava.code.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FlinkSQL 读取Kafka数据
 *
 */
public class FlinkSQLReadKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        conf.setString(RestOptions.BIND_PORT,"8081");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableConfig config = tableEnv.getConfig();
        config.getConfiguration().setString("table.exec.source.idle-timeout","5 s");
//        config.set("table.exec.source.idle-timeout","5 s");

        String  createSQL = "CREATE TABLE KafkaTable (" +
                "  `name` STRING," +
                "  `tt` INTEGER," +
                "  `ts` AS TO_TIMESTAMP(FROM_UNIXTIME(tt/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                "   WATERMARK FOR ts AS ts - INTERVAL '0' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'testtopic'," +
                "  'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")";
        //创建表
        tableEnv.executeSql(createSQL);

        tableEnv.executeSql("desc KafkaTable").print();



        String executeSql = "SELECT window_start, window_end, COUNT(name)" +
                "  FROM TABLE(" +
                "    HOP(TABLE KafkaTable, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '10' SECOND))" +
                "  GROUP BY window_start, window_end";
        TableResult tableResult = tableEnv.executeSql(executeSql);
        tableResult.print();
        tableEnv.executeSql("select * from KafkaTable").print();

//        TableResult tableResult = tableEnv.executeSql("select * from KafkaTable");
//        Table table = tableEnv.sqlQuery("select * from KafkaTable");

        env.execute();


    }
}
