package com.wubaibao.flinkjava.code.chapter9.tableapi;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table API OrderBy 、Offset、fetch 操作
 * 案例：读取socket中数据
 */
public class OrderByAndOffsetTest {
    public static void main(String[] args) {
        //准备Stream Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //准备TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        return new StationLog(
                                s.split(",")[0],
                                s.split(",")[1],
                                s.split(",")[2],
                                s.split(",")[3],
                                Long.valueOf(s.split(",")[4]),
                                Long.valueOf(s.split(",")[5]));
                    }
                });

        Table table = tableEnv.fromDataStream(
                ds,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build()
        );

        //按照duration升序排序，跳过前2条，获取之后的3条数据
        Table result = table.orderBy($("duration").asc()).offset(2).fetch(3);

        result.execute().print();

    }
}
