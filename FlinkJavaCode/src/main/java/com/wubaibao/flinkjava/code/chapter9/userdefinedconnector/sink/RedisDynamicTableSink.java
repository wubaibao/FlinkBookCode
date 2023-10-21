package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector.sink;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class RedisDynamicTableSink implements DynamicTableSink {

    private final String host;
    private final int port;
    private final int dbNum;

    /**
     * 创建RedisSinkFunction 构造
     */
    public RedisDynamicTableSink(String host,int port,int dbNum) {
        this.host = host;
        this.port = port;
        this.dbNum = dbNum;
    }

    /**
     * 设置运行期间包含的操作数据类型
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // 由Source端指定只有INSERT操作
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    /**
     * 获取运行时的 SinkFunction
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // 创建运行时类用于提交给集群
        RedisSinkFunction redisSinkFunction = new RedisSinkFunction(host, port, dbNum);
        // 返回 SinkFunctionProvider
        return SinkFunctionProvider.of(redisSinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(host, port, dbNum);
    }

    /**
     * 校验元数据出错时打印到控制台的信息 - 获取 Sink 信息
     */
    @Override
    public String asSummaryString() {
        return "Redis Table Sink";
    }
}
