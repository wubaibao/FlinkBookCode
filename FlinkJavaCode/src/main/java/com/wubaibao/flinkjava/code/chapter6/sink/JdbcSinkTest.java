package com.wubaibao.flinkjava.code.chapter6.sink;


import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;

/**
 * Flink JdbcSink 测试
 * 读取socket数据写入到MySQL中
 */
public class JdbcSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * socket 中输入数据如下：
         * 001,186,187,busy,1000,10
         * 002,187,186,fail,2000,20
         * 003,186,188,busy,3000,30
         * 004,188,186,busy,4000,40
         * 005,188,187,busy,5000,50
         */
        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map(one -> {
                    String[] arr = one.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });

        /**
         * mysql中创建的station_log 表结构如下；
         *
         * CREATE TABLE `station_log` (
         *   `sid` varchar(255) DEFAULT NULL,
         *   `call_out` varchar(255) DEFAULT NULL,
         *   `call_in` varchar(255) DEFAULT NULL,
         *   `call_type` varchar(255) DEFAULT NULL,
         *   `call_time` bigint(20) DEFAULT NULL,
         *   `duration` bigint(20) DEFAULT NULL
         * ) ;
         */

        //准备JDBC Sink对象
//        SinkFunction<StationLog> jdbcSink = JdbcSink.sink(
//                "insert into station_log(sid,call_out,call_in,call_type,call_time,duration) values(?,?,?,?,?,?)",
//                new JdbcStatementBuilder<StationLog>() {
//                    @Override
//                    public void accept(PreparedStatement pst, StationLog stationLog) throws SQLException {
//                        pst.setString(1, stationLog.getSid());
//                        pst.setString(2, stationLog.getCallOut());
//                        pst.setString(3, stationLog.getCallIn());
//                        pst.setString(4, stationLog.getCallType());
//                        pst.setLong(5, stationLog.getCallTime());
//                        pst.setLong(6, stationLog.getDuration());
//                    }
//                },
//                JdbcExecutionOptions.builder()
//                        //批次提交大小，默认500
//                        .withBatchSize(1000)
//                        //批次提交间隔间隔时间，默认0，即批次大小满足后提交
//                        .withBatchIntervalMs(1000)
//                        //最大重试次数，默认3
//                        .withMaxRetries(5)
//                        .build()
//                ,
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        //mysql8.0版本使用com.mysql.cj.jdbc.Driver
//                        .withUrl("jdbc:mysql://node2:3306/mydb?useSSL=false")
//                        .withDriverName("com.mysql.jdbc.Driver")
//                        .withUsername("root")
//                        .withPassword("123456")
//                        .build()
//        );

        SinkFunction<StationLog> jdbcSink = JdbcSink.sink(
                "insert into station_log(sid,call_out,call_in,call_type,call_time,duration) values(?,?,?,?,?,?)",
                (PreparedStatement pst, StationLog stationLog) -> {
                    pst.setString(1, stationLog.getSid());
                    pst.setString(2, stationLog.getCallOut());
                    pst.setString(3, stationLog.getCallIn());
                    pst.setString(4, stationLog.getCallType());
                    pst.setLong(5, stationLog.getCallTime());
                    pst.setLong(6, stationLog.getDuration());
                },
                JdbcExecutionOptions.builder()
                        //批次提交大小，默认500
                        .withBatchSize(1000)
                        //批次提交间隔间隔时间，默认0，即批次大小满足后提交
                        .withBatchIntervalMs(1000)
                        //最大重试次数，默认3
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        //mysql8.0版本使用com.mysql.cj.jdbc.Driver
                        .withUrl("jdbc:mysql://node2:3306/mydb?useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        //将数据写入到mysql中
        ds.addSink(jdbcSink);

        env.execute();
    }
}
