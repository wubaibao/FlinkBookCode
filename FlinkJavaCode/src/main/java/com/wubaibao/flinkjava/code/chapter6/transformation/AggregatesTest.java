package com.wubaibao.flinkjava.code.chapter6.transformation;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * Flink Aggregates 聚合算子测试
 * min/minBy/max/maxBy  都是针对KeyedStream
 */
public class AggregatesTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备集合数据
        List<StationLog> list = Arrays.asList(
                new StationLog("sid1", "18600000000", "18600000001", "success", System.currentTimeMillis(), 120L),
                new StationLog("sid1", "18600000001", "18600000002", "fail", System.currentTimeMillis(), 30L),
                new StationLog("sid1", "18600000002", "18600000003", "busy", System.currentTimeMillis(), 50L),
                new StationLog("sid1", "18600000003", "18600000004", "barring", System.currentTimeMillis(), 90L),
                new StationLog("sid1", "18600000004", "18600000005", "success", System.currentTimeMillis(), 300L)
        );

        KeyedStream<StationLog, String> keyedStream = env.fromCollection(list)
                .keyBy(stationLog -> stationLog.sid);
        //统计duration 的总和
        keyedStream.sum("duration").print();
        //统计duration的最小值，min返回该列最小值，其他列与第一条数据保持一致
        keyedStream.min("duration").print();
        //统计duration的最小值，minBy返回的是最小值对应的整个对象
        keyedStream.minBy("duration").print();
        //统计duration的最大值，max返回该列最大值，其他列与第一条数据保持一致
        keyedStream.max("duration").print();
        //统计duration的最大值，maxBy返回的是最大值对应的整个对象
        keyedStream.maxBy("duration").print();

        env.execute();

    }
}
