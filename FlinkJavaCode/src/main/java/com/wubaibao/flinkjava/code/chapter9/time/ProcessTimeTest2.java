package com.wubaibao.flinkjava.code.chapter9.time;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink Table API和SQL编程中指定 Processing Time
 * 案例:读取Socket中基站日志数据，每隔5秒进行数据条数统计
 */
public class ProcessTimeTest2 {
    public static void main(String[] args) {
        //创建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                    }
                });

        //将DataStream转换成Table
        Table table = tableEnv.fromDataStream(
                ds,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "PROCTIME()")
                        .build()
        );

        //通过SQL每隔5秒统计一次数据条数
        Table resultTbl = tableEnv.sqlQuery("" +
                "select " +
                "TUMBLE_START(proc_time,INTERVAL '5' SECOND) as window_start," +
                "TUMBLE_END(proc_time,INTERVAL '5' SECOND) as window_end," +
                "count(sid) as cnt " +
                "from " +table+
                " group by TUMBLE(proc_time,INTERVAL '5' SECOND)");

        //打印结果
        resultTbl.execute().print();

    }
}
