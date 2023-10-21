package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector.source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class SocketDynamicTableSource implements ScanTableSource {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;

    //实现类构造
    public SocketDynamicTableSource(
            String hostname,
            int port,
            byte byteDelimiter) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
    }


    /**
     * 设置运行期间包含的操作数据类型
     */
    @Override
    public ChangelogMode getChangelogMode() {
        // 由Source端指定只有INSERT操作
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    /**
     * 获取运行时的 SourceFunction
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        // 创建运行时类用于提交给集群
        final SourceFunction<RowData> sourceFunction = new SocketSourceFunction(
                hostname,
                port,
                byteDelimiter);

        return SourceFunctionProvider.of(sourceFunction, false);
    }


    /**
     * 内部执行计划需要复制表源的实例，需要实现此方法
     */
    @Override
    public DynamicTableSource copy() {
        return new SocketDynamicTableSource(hostname, port, byteDelimiter);
    }


    /**
     * 校验元数据出错时打印到控制台的信息 - 获取 Source 信息
     */
    @Override
    public String asSummaryString() {
        return "Socket Table Source";
    }
}
