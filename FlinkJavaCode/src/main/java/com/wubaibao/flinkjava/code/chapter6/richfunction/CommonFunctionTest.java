package com.wubaibao.flinkjava.code.chapter6.richfunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * Flink 普通函数接口测试
 */
public class CommonFunctionTest {
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
        ds.map(new MyMapFunction()).print();

        env.execute();

    }

    private static class MyMapFunction implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            //value格式：001,186,187,busy,1000,10
            String[] split = value.split(",");
            //获取通话时间，并转换成yyyy-MM-dd HH:mm:ss格式
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String startTime = sdf.format(Long.parseLong(split[4]));

            //获取通话时长，通话时间加上通话时长，得到通话结束时间，转换成yyyy-MM-dd HH:mm:ss格式
            String duration = split[5];
            String endTime = sdf.format(Long.parseLong(split[4]) + Long.parseLong(duration));

            return "基站ID:" + split[0] + ",主叫号码:" + split[1] + "," +
                    "被叫号码:" + split[2] + ",通话类型:" + split[3] + "," +
                    "通话开始时间:" + startTime + ",通话结束时间:" + endTime ;
        }
    }

}
