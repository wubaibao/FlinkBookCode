package com.wubaibao.flinkjava.code.chapter9.tableapi.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 *
 * Flink Table API - IntervalJoin测试
 * 案例：读取用户登录流和广告点击流，通过IntervalJoin分析用户点击广告的行为。
 *
 */
public class IntervalJoinTest {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取socket-8888 中用户login 数据：user_1,6000
        SingleOutputStreamOperator<Tuple2<String, Long>> loginDS = env.socketTextStream("node5", 8888)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple2.of(split[0], Long.valueOf(split[1]));
                    }
                });

        //读取socket-9999中的点击广告数据：user_1,product_1,3000
        SingleOutputStreamOperator<Tuple3<String, String, Long>> clickDS =
                env.socketTextStream("node5", 9999)
                        .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                            @Override
                            public Tuple3<String, String, Long> map(String s) throws Exception {
                                String[] split = s.split(",");
                                return Tuple3.of(split[0], split[1], Long.valueOf(split[2]));
                            }
                        });

        //将DS转换成Table
        Table loginTable = tableEnv.fromDataStream(loginDS,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f1,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
                        .build()
        ).as("left_uid", "left_dt", "left_rowtime");

        Table clickTable = tableEnv.fromDataStream(clickDS,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BIGINT())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f2,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
                        .build()
        ).as("right_uid", "right_adv", "right_dt","right_rowtime");

        loginTable.printSchema();
        clickTable.printSchema();

        //interval Join
        Table result = loginTable.join(clickTable)
                .where(
                        and(
                                $("left_uid").isEqual($("right_uid")),
                                $("right_rowtime").isGreaterOrEqual($("left_rowtime").minus(lit(2).second())),
                                $("right_rowtime").isLess($("left_rowtime").plus(lit(2).second()))
                        )
                ).select(
                        $("left_uid"),
                        $("left_rowtime"),
                        $("right_adv"),
                        $("right_rowtime")
                );

        result.execute().print();

    }
}
