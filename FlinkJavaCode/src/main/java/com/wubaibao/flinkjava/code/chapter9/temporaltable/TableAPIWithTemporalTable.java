package com.wubaibao.flinkjava.code.chapter9.temporaltable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Flink Table API  - 创建时态表并查询数据
 */
public class TableAPIWithTemporalTable {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //读取socket中数据 ,p_001,1000
        SingleOutputStreamOperator<Tuple2<String, Long>> leftInfo =
                env.socketTextStream("node5", 8888)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return Tuple2.of(
                                s.split(",")[0],
                                Long.valueOf(s.split(",")[1])
                        );
                    }
                });

        Table leftTbl = tableEnv.fromDataStream(leftInfo,
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
                                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f1,3)")
                                //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
                                .watermark("rowtime", "rowtime - INTERVAL '5' SECOND")
                                .build())
                .as("left_product_id","left_dt","left_rowtime");

        //读取socket中数据 ,p_001,1000
        SingleOutputStreamOperator<Tuple4<Long,String,String,Double>> rightInfo =
                env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, Tuple4<Long,String,String,Double>>() {
                    @Override
                    public Tuple4<Long,String,String,Double> map(String s) throws Exception {
                        return Tuple4.of(
                                Long.valueOf(s.split(",")[0]),
                                s.split(",")[1],
                                s.split(",")[2],
                                Double.valueOf(s.split(",")[3])
                        );
                    }
                });

        Table rightTbl = tableEnv.fromDataStream(rightInfo,
                        Schema.newBuilder()
                                .column("f0", DataTypes.BIGINT())
                                .column("f1", DataTypes.STRING())
                                .column("f2", DataTypes.STRING())
                                .column("f3", DataTypes.DOUBLE())
                                //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
                                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f0,3)")
                                //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
                                .watermark("rowtime", "rowtime - INTERVAL '5' SECOND")
                                .build())
                .as("right_update_time", "right_product_id", "right_product_name", "right_price", "right_rowtime");

        //创建时态表函数，"right_rowtime"为时间属性，"right_product_id"为主键
        TemporalTableFunction temporalTableFunction = rightTbl.createTemporalTableFunction($("right_rowtime"), $("right_product_id"));
        tableEnv.createTemporarySystemFunction("temporalTableFunction", temporalTableFunction);

        //使用时态表
        Table result = leftTbl.joinLateral(
                        call("temporalTableFunction", $("left_rowtime")),
                        $("left_product_id").isEqual($("right_product_id"))
                )
                .select($("left_product_id"), $("left_dt"),$("right_product_name"),$("right_price") );

        result.execute().print();

    }
}
