package com.wubaibao.flinkjava.code.chapter7.keyedstate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink ListState 测试
 * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长
 */
public class ListStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * Socket中数据如下:
         *  001,186,187,busy,1000,10
         *  002,187,186,fail,2000,20
         *  003,186,188,busy,3000,30
         *  004,187,186,busy,4000,40
         *  005,189,187,busy,5000,50
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //对ds进行转换处理，得到StationLog对象
        SingleOutputStreamOperator<StationLog> stationLogDS = ds.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String line) throws Exception {
                String[] arr = line.split(",");
                return new StationLog(
                        arr[0].trim(),
                        arr[1].trim(),
                        arr[2].trim(),
                        arr[3].trim(),
                        Long.valueOf(arr[4]),
                        Long.valueOf(arr[5])
                );
            }
        });

        //这里要用到 定时器，所以采用process API
        SingleOutputStreamOperator<String> result = stationLogDS.keyBy(stationLog -> stationLog.callOut)
                .process(new KeyedProcessFunction<String, StationLog, String>() {
            //定义一个ListState 用来存放同一个主叫号码的所有通话时长
            private ListState<Long> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义状态描述器
                ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<Long>("listState", Long.class);
                //获取ListState
                listState = getRuntimeContext().getListState(listStateDescriptor);
            }

            @Override
            public void processElement(StationLog value, KeyedProcessFunction<String, StationLog, String>.Context ctx, Collector<String> out) throws Exception {
                //获取ListState中的数据
                Iterable<Long> iterable = listState.get();
                //如果iterable中没有数据，说明是第一条数据，需要注册一个定时器
                if (!iterable.iterator().hasNext()) {
                    //获取当前处理数据时间
                    long time = ctx.timerService().currentProcessingTime();
                    //注册一个20s后的定时器
                    ctx.timerService().registerProcessingTimeTimer(time + 20 * 1000L);
                }

                //将通话时长存入ListState
                listState.add(value.duration);
            }

            /**
             * 定时器触发时，对同一个主叫号码的所有通话时长进行求和
             */
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, StationLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                //获取ListState中的所有通话时长
                Iterable<Long> iterable = listState.get();

                //定义一个变量，用来存放通话时长总和
                long totalCallTime = 0L;

                //遍历迭代器，对通话时长进行求和
                for (Long duration : iterable) {
                    totalCallTime += duration;
                }

                //输出结果
                out.collect("主叫号码：" + ctx.getCurrentKey() + "，近20s通话时长：" + totalCallTime);

                //清空ListState
                listState.clear();
            }
        });

        result.print();
        env.execute();

    }
}
