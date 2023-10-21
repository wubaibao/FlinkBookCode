package com.wubaibao.flinkjava.code.chapter6.source;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义非并行Source
 */
class MyDefinedNoParalleSource implements SourceFunction<StationLog>{
    Boolean flag = true;

    /**
     * 主要方法:启动一个Source，大部分情况下都需要在run方法中实现一个循环产生数据
     * 这里计划每秒产生1条基站数据
     */
    @Override
    public void run(SourceContext<StationLog> ctx) throws Exception {
        Random random = new Random();
        String[] callTypes = {"fail","success","busy","barring"};
        while(flag){
            String sid = "sid_"+random.nextInt(10);
            String callOut = "1811234"+(random.nextInt(9000)+1000);
            String callIn = "1915678"+(random.nextInt(9000)+1000);
            String callType = callTypes[random.nextInt(4)];
            Long callTime = System.currentTimeMillis();
            Long durations = Long.valueOf(random.nextInt(50)+"");
            ctx.collect(new StationLog(sid,callOut,callIn,callType,callTime,durations));
            Thread.sleep(5000);//1s 产生一个事件
        }

    }

    //当取消对应的Flink任务时被调用
    @Override
    public void cancel() {
        flag = false;
    }
}

/**
 * Flink读取自定义Source，并行度为1
 */
public class NoParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<StationLog> dataStream = env.addSource(new MyDefinedNoParalleSource());
        dataStream.print();
        env.execute();
    }
}
