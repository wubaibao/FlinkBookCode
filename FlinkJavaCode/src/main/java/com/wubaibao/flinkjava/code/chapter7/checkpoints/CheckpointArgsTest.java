package com.wubaibao.flinkjava.code.chapter7.checkpoints;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Checkpoint参数测试
 */
public class CheckpointArgsTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，每隔1000ms进行一次checkpoint
        env.enableCheckpointing(1000);

        //设置checkpoint storage存储为JobManagerStorage，默认堆内存存储状态大小为5M
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage(5*1024*1024));

        //设置checkpoint storage存储为hdfs路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/flink/checkpoints");

        //设置检查点模式为exactly-once，默认值为exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置检查点模式为at-least-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        //设置Checkpoint 超时时间，默认值为10分钟
        env.getCheckpointConfig().setCheckpointTimeout(10*60*1000);

        //设置 checkpoint 最小间隔时间为500ms，默认值为0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        //设置checkpoint最大并行度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //设置可容忍checkpoint失败次数,没有默认值值，表示不容忍任何checkpoint失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        /**
         * 设置checkpoint的清理策略，当作业取消时，checkpoint数据的保留策略，默认值为RETAIN_ON_CANCELLATION
         * RETAIN_ON_CANCELLATION：当作业取消时，保留checkpoint数据
         * DELETE_ON_CANCELLATION：当作业取消时，删除checkpoint数据
         */
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //设置 checkpoint barrier 不对齐机制，设置此值时，checkpointmode必须为EXACTLY_ONCE且MaxConcurrentCheckpoints为1
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        //开启Flink任务完成后进行checkpoint检查点，默认值为true
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env.configure(config);


    }
}
