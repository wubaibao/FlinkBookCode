package com.wubaibao.flinkjava.code.chapter9.time;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink Table API和SQL编程中指定 Event Time
 * 案例:读取Kafka中基站日志数据，每隔5秒进行数据条数统计
 */
public class EventTimeTest2 {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //读取socket中基站日志数据并转换为StationgLog类型DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = env.socketTextStream("node5", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });

        Table table = tableEnv.fromDataStream(stationLogDS,
                Schema.newBuilder()
                        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build());

        //通过SQL每隔5秒统计一次数据条数
        Table resultTbl = tableEnv.sqlQuery("" +
                "select " +
                "TUMBLE_START(rowtime,INTERVAL '5' SECOND) as window_start," +
                "TUMBLE_END(rowtime,INTERVAL '5' SECOND) as window_end," +
                "count(sid) as cnt " +
                "from " +table+
                " group by TUMBLE(rowtime,INTERVAL '5' SECOND)");

        //打印结果
        resultTbl.execute().print();
    }
}
