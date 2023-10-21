package com.wubaibao.flinkjava.code.chapter6.richfunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Flink RichFunction 测试
 */
public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Socket中的数据格式如下:
         *  001,186,187,busy,1000,10
         *  002,187,186,fail,2000,20
         *  003,186,188,busy,3000,30
         *  004,188,186,busy,4000,40
         *  005,188,187,busy,5000,50
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        ds.map(new MyRichMapFunction()).print();

        env.execute();

    }

    private static class MyRichMapFunction extends RichMapFunction<String,String> {

        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet rst = null;

        // open()方法在map方法之前执行，用于初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false","root","123456");
            pst = conn.prepareStatement("select * from person_info where phone_num = ?");
        }

        //character-set-server=utf8

        // map方法，输入一个元素，返回一个元素
        @Override
        public String map(String value) throws Exception {
            //value 格式：001,186,187,busy,1000,10
            String[] split = value.split(",");
            String sid = split[0];
            String callOut = split[1];//主叫
            String callIn = split[2];//被叫
            String callType = split[3];//通话类型
            String callTime = split[4];//通话时间
            String duration = split[5];//通话时长
            //mysql中获取主叫和被叫的姓名
            String callOutName = "";
            String callInName = "";

            pst.setString(1,callOut);
            rst = pst.executeQuery();
            while (rst.next()){
                callOutName = rst.getString("name");
            }
            pst.setString(1,callIn);
            rst = pst.executeQuery();
            while (rst.next()){
                callInName = rst.getString("name");
            }

            return "基站ID:" + sid + ",主叫号码:" + callOut + ",主叫姓名:" + callOutName + "," +
                    "被叫号码:" + callIn + ",被叫姓名:" + callInName + ",通话类型:" + callType + "," +
                    "通话时间:" + callTime + ",通话时长:" + duration+"s";
        }

        // close()方法在map方法之后执行，用于清理
        @Override
        public void close() throws Exception {
            rst.close();
            pst.close();
            conn.close();

        }

    }
}
