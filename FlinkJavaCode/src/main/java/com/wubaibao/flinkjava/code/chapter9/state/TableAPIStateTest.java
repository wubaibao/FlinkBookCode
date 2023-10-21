package com.wubaibao.flinkjava.code.chapter9.state;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * Flink Table API中状态测试
 * 案例：读取socket中基站日志数据，按照基站进行分组统计基站的通话时长
 */
public class TableAPIStateTest {
    public static void main(String[] args) {
        //获取DataStream的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取Table API的运行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置状态保存时间，默认为0，表示不清理状态，这里设置5s
        tableEnv.getConfig().set("table.exec.state.ttl","5000");

        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new StationLog(split[0], split[1], split[2], split[3], Long.parseLong(split[4]), Long.parseLong(split[5]));
                    }
                });

        //将DataStream转换成Table
        tableEnv.createTemporaryView("station_tbl",ds);

        //通过SQL统计通话数据信息
        Table result = tableEnv.sqlQuery("select sid,sum(duration) as total_duration from station_tbl group by sid");

        //打印输出
        result.execute().print();

    }
}
