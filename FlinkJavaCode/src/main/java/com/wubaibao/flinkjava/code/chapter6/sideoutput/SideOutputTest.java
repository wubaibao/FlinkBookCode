package com.wubaibao.flinkjava.code.chapter6.sideoutput;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink 侧输出流测试
 */
public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Socket中的数据格式如下:
         *  001,186,187,success,1000,10
         *  002,187,186,fail,2000,20
         *  003,186,188,success,3000,30
         *  004,188,186,success,4000,40
         *  005,188,187,busy,5000,50
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //定义侧输出流的标签
        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        SingleOutputStreamOperator<String> mainStream = ds.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                //value 格式：001,186,187,success,1000,10
                String[] split = value.split(",");
                String callType = split[3];//通话类型
                //判断通话类型
                if ("success".equals(callType)) {
                    //成功类型，输出到主流
                    out.collect(value);
                } else {
                    //其他类型，输出到侧输出流
                    ctx.output(outputTag, value);
                }

            }
        });

        //获取主流
        mainStream.print("主流");

        //获取侧输出流
        mainStream.getSideOutput(outputTag).print("侧输出流");

        env.execute();

    }

}
