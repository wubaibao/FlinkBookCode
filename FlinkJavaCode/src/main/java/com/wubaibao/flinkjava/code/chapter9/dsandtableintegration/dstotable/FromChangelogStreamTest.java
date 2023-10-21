package com.wubaibao.flinkjava.code.chapter9.dsandtableintegration.dstotable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * Flink DataStream 转换为 Table
 * 使用 tableEnv.fromChangelogStream() 方法将 DataStream 转换为 Table
 */
public class FromChangelogStreamTest {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为了看出数据顺序效果，设置并行度为1
        env.setParallelism(1);

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "zs", 18),
                        Row.ofKind(RowKind.INSERT, "ls", 19),
                        Row.ofKind(RowKind.INSERT, "ww", 20),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "zs", 18),
                        Row.ofKind(RowKind.UPDATE_AFTER, "zs", 20),
                        Row.ofKind(RowKind.DELETE, "ls", 19),
                        Row.ofKind(RowKind.UPDATE_AFTER, "ww", 21),
                        Row.ofKind(RowKind.INSERT, "zs", 18)
                        );

        //将DataStream 转换成 Table
        Table result = tableEnv.fromChangelogStream(dataStream,
                //通过Schema指定主键
                Schema.newBuilder().primaryKey("f0").build(),
                //指定ChangelogMode，这里使用all()，表示所有类型的数据都会被处理
                ChangelogMode.all()
        );

        //打印表结构
        result.execute().print();
    }
}
