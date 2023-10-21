package com.wubaibao.flinkjava.code.chapter12;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Checkpoint 多并行执行
 */
public class CheckpointParalleCodeTest {
    public static void main(String[] args) throws Exception {
        //1.使用本地模式
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8081");
        //使用配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //设置checkpoint
        env.enableCheckpointing(1000);

        //默认就是exactly-once语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(100);

        DataStreamSource<StationLog> source = env.addSource(new RichParallelSourceFunction<StationLog>() {
            Boolean flag = true;

            /**
             * 主要方法:启动一个Source，大部分情况下都需要在run方法中实现一个循环产生数据
             * 这里计划1s 产生1条基站数据，由于是并行，当前节点有几个core就会有几条数据
             */
            @Override
            public void run(SourceContext<StationLog> ctx) throws Exception {
                Random random = new Random();
                String[] callTypes = {"fail", "success", "busy", "barring"};
                while (flag) {
                    String sid = "sid_" + random.nextInt(10);
                    String callOut = "1811234" + (random.nextInt(9000) + 1000);
                    String callIn = "1915678" + (random.nextInt(9000) + 1000);
                    String callType = callTypes[random.nextInt(4)];
                    Long callTime = System.currentTimeMillis();
                    Long durations = Long.valueOf(random.nextInt(50) + "");
                    ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));
                    Thread.sleep(1000);//1s 产生一个事件
                }

            }

            //当取消对应的Flink任务时被调用
            @Override
            public void cancel() {
                flag = false;
            }
        });

        //处理数据
        SingleOutputStreamOperator<String> result =
                source.keyBy(stationLog -> stationLog.sid)
                .map(new RichMapFunction<StationLog, String>() {

            private ListState<String> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("liststate", String.class);
                listState = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public String map(StationLog value) throws Exception {
                //100倍状态存储
                for(int i = 0 ;i<100 ;i++){
                    listState.add(value.toString());
                }
                //每条数据都暂停处理 500ms
                Thread.sleep(500);

                return value.toString();
            }

        });

        result.print();

        env.execute();

    }
}