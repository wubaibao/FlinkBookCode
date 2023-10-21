package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisSinkFunction extends RichSinkFunction<RowData> {

    private final String host;
    private final int port;
    private final int dbNum;

    private Jedis jedis;

    /**
     * 创建RedisSinkFunction 构造
     */
    public RedisSinkFunction(String host,int port,int dbNum){
        this.host = host;
        this.port = port;
        this.dbNum = dbNum;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis(host,port);
        jedis.select(dbNum);
    }

    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        // 从RowData中获取数据
        String sid = rowData.getString(0).toString();
        String windowStart = rowData.getString(1).toString();
        String windowEnd = rowData.getString(2).toString();
        String totalDur = rowData.getLong(3)+"";

        //Redis中数据存储格式如下：（窗口时间范围，基站ID，基站通话总时长）
        jedis.hset(windowStart+"~"+windowEnd,sid,totalDur);
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }

}
