package com.wubaibao.flinkjava.code.chapter9.tableapi.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table API - fullOuterJoin测试
 * 案例：读取socket中数据形成两个流，进行fullOuterJoin操作
 */
public class FullOuterJoinTest {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取socket-8888中基站日志数据并转换为Tuple类型DataStream
        //1,zs,18,1000
        SingleOutputStreamOperator<Tuple4<Integer, String, Integer, Long>> personInfo =
                env.socketTextStream("node5", 8888)
                .map(new MapFunction<String, Tuple4<Integer, String, Integer, Long>>() {
                    @Override
                    public Tuple4<Integer, String, Integer, Long> map(String s) throws Exception {
                        return Tuple4.of(
                                Integer.valueOf(s.split(",")[0]),
                                s.split(",")[1],
                                Integer.valueOf(s.split(",")[2]),
                                Long.valueOf(s.split(",")[3]));
                    }
                });

        //读取socket-9999中基站日志数据并转换为Tuple类型DataStream
        //1,zs,beijing,1000
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Long>> addressInfo =
                env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, Tuple4<Integer, String, String, Long>>() {
                    @Override
                    public Tuple4<Integer, String, String, Long> map(String s) throws Exception {
                        return Tuple4.of(
                                Integer.valueOf(s.split(",")[0]),
                                s.split(",")[1],
                                s.split(",")[2],
                                Long.valueOf(s.split(",")[3]));
                    }
                });

        Table personTbl = tableEnv.fromDataStream(personInfo,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.INT())
                        .column("f3", DataTypes.BIGINT())
                        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f3,3)")
                        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build())
                .as("left_id","left_name","age","left_rowtime","left_ltz_time");

        Table addressTbl = tableEnv.fromDataStream(addressInfo,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.STRING())
                        .column("f3", DataTypes.BIGINT())
                        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f3,3)")
                        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build())
                .as("right_id","right_name","address","right_rowtime","right_ltz_time");

        //打印schema
        personTbl.printSchema();
        addressTbl.printSchema();

        //fullOuterJoin
        Table resultTbl = personTbl.fullOuterJoin(
                addressTbl,$("left_id").isEqual($("right_id")))
                .select(
                        $("left_id"),
                        $("left_name"),
                        $("age"),
                        $("address"),
                        $("left_rowtime"),
                        $("right_rowtime"));

        resultTbl.execute().print();

    }
}
