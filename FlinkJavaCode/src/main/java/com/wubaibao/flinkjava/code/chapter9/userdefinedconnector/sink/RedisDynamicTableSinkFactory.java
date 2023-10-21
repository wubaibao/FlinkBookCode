package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * Flink 自定义 Sink Connector - Redis SinkConnector
 */
public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {

    // 定义所有配置项
    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Integer> DBNUM = ConfigOptions.key("db-num")
            .intType()
            .defaultValue(0);

    /**
     * 用于匹配： `connector = '...'`，必须和 `connector` 的值一致，否则会报错，找不到对应的Connector
     */
    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        //return Collections.emptySet();
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DBNUM);
        return options;
    }

    /**
     * 创建并返回 DynamicTableSource
     */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // 使用提供的工具类或实现你自己的逻辑进行校验
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // 校验所有的配置项
        helper.validate();

        // 获取校验完的配置项
        final ReadableConfig options = helper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final int port = options.get(PORT);
        final int dbNum = options.get(DBNUM);

        // 创建并返回动态表 Sink
        return new RedisDynamicTableSink(hostname, port, dbNum);
    }
}
