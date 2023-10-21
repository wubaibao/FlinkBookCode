package com.wubaibao.flinkjava.code.chapter6.partitions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Flink Forward 分区测试
 */
public class ForwardTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        conf.setString(RestOptions.BIND_PORT,"8081");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<Integer> ds1 = env.addSource(new RichParallelSourceFunction<Integer>() {
            Boolean flag = true;

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
                for (Integer integer : integers) {
                    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                    if (integer % 2 != 0 && indexOfThisSubtask == 0) {
                        ctx.collect(integer);
                    } else if (integer % 2 == 0 && indexOfThisSubtask == 1) {
                        ctx.collect(integer);
                    }
                }

            }

            @Override
            public void cancel() {
                flag = false;
            }
        });
        ds1.print("ds1");

        SingleOutputStreamOperator<String> ds2 = ds1.forward().map(one -> one + "xx");
        ds2.print("ds2");

        env.execute();

    }

}
