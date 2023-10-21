package com.wubaibao.flinkjava.code.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

/**
 * Flink 读取MySQL数据
 */
public class FlinkReadMySQLExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.addSource(new RichSourceFunction<String>() {
            private Connection conn = null;
            private PreparedStatement pst = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://node2:3306/testdb","root","123456");
                pst = conn.prepareStatement("select id ,name, age from person");
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                ResultSet rst = pst.executeQuery();
                while (rst.next()) {
                    String id = rst.getString(1);
                    String name = rst.getString(2);
                    String age = rst.getString(3);
                    ctx.collect(id + "-" + name + "-" + age);
                }

            }

            @Override
            public void cancel() {
                try {
                    pst.close();
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        });
        ds.print();
        env.execute();
    }
}
