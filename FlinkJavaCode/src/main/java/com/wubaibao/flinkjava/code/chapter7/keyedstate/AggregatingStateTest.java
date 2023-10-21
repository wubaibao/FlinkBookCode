package com.wubaibao.flinkjava.code.chapter7.keyedstate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink AggregatingState 状态编程
 * 案例：读取基站日志数据，统计每个主叫号码通话平均时长。
 */
public class AggregatingStateTest {
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
        stationLogDS.keyBy(stationLog -> stationLog.callOut)
                .map(new RichMapFunction<StationLog, String>() {
                    //定义一个AggregatingState 用来存放同一个主叫号码的所有通话次数和通话时长
                    private AggregatingState<StationLog,Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义状态描述器
                        AggregatingStateDescriptor<StationLog, Tuple2<Long, Long>, Double> aggregatingStateDescriptor =
                                new AggregatingStateDescriptor<>("name",
                                new AggregateFunction<StationLog, Tuple2<Long, Long>, Double>() {
                                    //创建累加器，第一个位置存放通话次数，第二个位置存放总通话时长
                                    @Override
                                    public Tuple2<Long, Long> createAccumulator() {
                                        return new Tuple2<>(0L, 0L);
                                    }

                                    //每来一条数据，调用一次add方法
                                    @Override
                                    public Tuple2<Long, Long> add(StationLog stationLog, Tuple2<Long, Long> acc) {
                                        return new Tuple2<>(acc.f0 + 1, acc.f1 + stationLog.duration);
                                    }

                                    //返回结果
                                    @Override
                                    public Double getResult(Tuple2<Long, Long> acc) {
                                        //将通话次数和通话时长相除，得到平均通话时长
                                        return Double.valueOf(acc.f1) / Double.valueOf(acc.f0);
                                    }

                                    //合并两个累加器
                                    @Override
                                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
                                        return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                                    }
                                },
                                Types.TUPLE(Types.LONG, Types.LONG));

                        //根据状态描述器获取状态
                        aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
                    }

                    @Override
                    public String map(StationLog stationLog) throws Exception {
                        aggregatingState.add(stationLog);
                        return "主叫手机号码：" + stationLog.callOut + "，平均通话时长：" + aggregatingState.get()+"秒";
                    }
                }).print();

        env.execute();

    }
}
