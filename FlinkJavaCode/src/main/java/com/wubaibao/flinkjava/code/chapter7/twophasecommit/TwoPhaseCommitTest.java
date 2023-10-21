package com.wubaibao.flinkjava.code.chapter7.twophasecommit;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.*;

/**
 * 两阶段提交测试 - TwoPhaseCommitSinkFunction 类实现
 * 案例：通过两阶段提交实现类完成读取Kafka数据写入到MySQL
 */
public class TwoPhaseCommitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启checkpoint
        env.enableCheckpointing(5000);

        /**
         * Kafka中输入数据如下:
         * 1,zs,18
         * 2,ls,20
         * 3,ww,19
         * 4,zl,21
         * 5,tq,22
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092") //设置Kafka 集群节点
                .setTopics("2pc-topic") //设置读取的topic
                .setGroupId("my-test-group") //设置消费者组
                .setStartingOffsets(OffsetsInitializer.latest()) //设置读取数据位置
                .setValueOnlyDeserializer(new SimpleStringSchema()) //设置value的序列化格式
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka-source");

        //自定义Sink 到MySQL,通过实现TwoPhaseCommitSinkFunction接口
        kafkaDS.addSink(new CustomTwoPhaseCommitSinkFunction());

        env.execute();

    }

}

/**
 * 自定义 Sink 到 MySQL,通过继承TwoPhaseCommitSinkFunction抽象类
 * TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT>
 *     IN: 输入数据类型
 *     TXN: 当前事务流程中的对象，贯穿TwoPhaseCommitSinkFunction类中各个方法中
 *     CONTEXT: 上下文类型
 */
class CustomTwoPhaseCommitSinkFunction extends TwoPhaseCommitSinkFunction<String, JdbcCommonUtils, Void> {

    //创建默认的构造方法
    public CustomTwoPhaseCommitSinkFunction() {
        super(new KryoSerializer<>(JdbcCommonUtils.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 开始事务。这里Flink会调用beginTransaction()方法创建数据库工具对象
     */
    @Override
    protected JdbcCommonUtils beginTransaction() throws Exception {
        System.out.println("beginTransaction()...");
        return new JdbcCommonUtils();
    }

    /**
     * 每接收一条数据后，会调用invoke()方法，将数据写入到MySQL中
     */
    @Override
    protected void invoke(JdbcCommonUtils jdbcUtils, String value, Context context) throws Exception {
        //将数据写入到MySQL中
        String[] split = value.split(",");
        PreparedStatement pst = jdbcUtils.getConnect().prepareStatement("insert into user(id,name,age) values(?,?,?)");
        pst.setInt(1,Integer.valueOf(split[0]));
        pst.setString(2,split[1]);
        pst.setInt(3,Integer.valueOf(split[2]));

        //执行插入操作
        pst.execute();

        //关闭pst对象
        pst.close();
    }

    /**
     * 当barrier 到达后，Flink会调用preCommit()方法，进行数据预提交
     * 预提交，如果一个preCommit执行失败，其他preCommit也会失败，Flink会调用abort()方法
     */
    @Override
    protected void preCommit(JdbcCommonUtils jdbcCommonUtils) throws Exception {
        System.out.println("barrier 到达，preCommit() 方法执行，开始预提交...");
        //这里的逻辑放在invoke()方法中进行插入数据
    }

    /**
     * Flink checkpoint完成，真正执行提交，Flink在notifyCheckpointComplete()方法中调用该方法，即JobManager完成checkpoint后调用该方法
     */
    @Override
    protected void commit(JdbcCommonUtils jdbcCommonUtils) {
        System.out.println("commit() 方法执行...");
        //提交事务
        jdbcCommonUtils.commit();
    }

    /**
     * 代码出现异常，事务中止，Flink会调用abort()方法
     * 这里主要是回滚事务
     */
    @Override
    protected void abort(JdbcCommonUtils jdbcCommonUtils) {
        System.out.println("abort() 方法执行...");
        //回滚事务
        jdbcCommonUtils.rollback();
        //关闭连接
        jdbcCommonUtils.close();
    }
}
