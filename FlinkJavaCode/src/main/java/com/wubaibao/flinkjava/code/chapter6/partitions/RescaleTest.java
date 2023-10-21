package com.wubaibao.flinkjava.code.chapter6.partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Flink Rescaling 重平衡分区测试
 */
public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Object> ds = env.addSource(new RichParallelSourceFunction<Object>() {
            @Override
            public void run(SourceContext<Object> ctx) throws Exception {
                List<String> list1 = Arrays.asList("a", "b", "c", "d", "e", "f");
                List<Integer> list2 = Arrays.asList(1, 2, 3, 4, 5, 6);
                for (String elem : list1) {
                    //这里的getRuntimeContext().getIndexOfThisSubtask()是获取当前subtask的index，从0开始
                    if (0 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(elem);
                    }
                }
                for (Integer elem : list2) {
                    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                    if (1 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(elem);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });

        //比较rescale和rebalance的区别
        ds.rescale().print("rescale").setParallelism(3);
//        ds.rebalance().print("reblance").setParallelism(4);
        env.execute();
    }
}
