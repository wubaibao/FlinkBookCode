//package com.wubaibao.flinkjava.code.chapter6.sink;
//
//import com.wubaibao.flinkjava.code.chapter6.StationLog;
//import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
//import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.util.function.SerializableSupplier;
//
//import javax.sql.XADataSource;
//import java.sql.PreparedStatement;
//
///**
// * Flink JdbcSink ExactlyOnce 测试
// */
//public class JdbcSinkExactlyOnceTest {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //必须设置checkpoint，否则数据不能正常写出到mysql
//        env.enableCheckpointing(5000);
//        /**
//         * socket 中输入数据如下：
//         * 001,186,187,busy,1000,10
//         * 002,187,186,fail,2000,20
//         * 003,186,188,busy,3000,30
//         * 004,188,186,busy,4000,40
//         * 005,188,187,busy,5000,50
//         */
//        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
//                .map(one -> {
//                    String[] arr = one.split(",");
//                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
//                });
//
//        //设置JdbcSink ExactlyOnce 对象
//        SinkFunction<StationLog> jdbcExactlyOnceSink = JdbcSink.exactlyOnceSink(
//                "insert into station_log(sid,call_out,call_in,call_type,call_time,duration) values(?,?,?,?,?,?)",
//                (PreparedStatement pst, StationLog stationLog) -> {
//                    pst.setString(1, stationLog.getSid());
//                    pst.setString(2, stationLog.getCallOut());
//                    pst.setString(3, stationLog.getCallIn());
//                    pst.setString(4, stationLog.getCallType());
//                    pst.setLong(5, stationLog.getCallTime());
//                    pst.setLong(6, stationLog.getDuration());
//                },
//                JdbcExecutionOptions.builder()
//                        //批次提交大小，默认500
//                        .withBatchSize(1000)
//                        //批次提交间隔间隔时间，默认0，即批次大小满足后提交
//                        .withBatchIntervalMs(1000)
//                        //最大重试次数，默认3,JDBC XA接收器要求maxRetries等于0，否则可能导致重复。
//                        .withMaxRetries(0)
//                        .build(),
//                JdbcExactlyOnceOptions.builder()
//                        //只允许每个连接有一个 XA 事务
//                        .withTransactionPerConnection(true)
//                        .build(),
////                //创建XA DataSource对象
////                new SerializableSupplier<XADataSource>() {
////                    @Override
////                    public XADataSource get() {
////                        MysqlXADataSource xaDataSource = new com.mysql.jdbc.jdbc2.optional.MysqlXADataSource();
////                        xaDataSource.setUrl("jdbc:mysql://node2:3306/mydb?useSSL=false");
////                        xaDataSource.setUser("root");
////                        xaDataSource.setPassword("123456");
////                        return xaDataSource;
////                    }
////                }
//                //创建XA DataSource对象也可以使用lambda表达式
//                () -> {
//                    MysqlXADataSource xaDataSource = new MysqlXADataSource();
//                    xaDataSource.setUrl("jdbc:mysql://node2:3306/mydb?useSSL=false");
//                    xaDataSource.setUser("root");
//                    xaDataSource.setPassword("123456");
//                    return xaDataSource;
//                }
//
//        );
//
//        //数据写出到mysql
//        ds.addSink(jdbcExactlyOnceSink);
//
//        env.execute();
//    }
//}
