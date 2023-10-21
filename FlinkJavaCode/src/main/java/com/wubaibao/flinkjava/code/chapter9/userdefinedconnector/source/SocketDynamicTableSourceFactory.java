package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;


public class SocketDynamicTableSourceFactory implements DynamicTableSourceFactory {

    // 定义所有配置项
    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue();

    //设置输入socket中每行的分割符 ，默认是\n
    public static final ConfigOption<Integer> BYTE_DELIMITER = ConfigOptions.key("byte-delimiter")
            .intType()
            .defaultValue(10); // 等同于 '\n'

    /**
     * 用于匹配： `connector = '...'`，必须和 `connector` 的值一致，否则会报错，找不到对应的Connector
     */
    @Override
    public String factoryIdentifier() {
        return "socket";
    }

    /**
     * 在Connector中必须包含的配置项
     * 后续会通过 helper.validate() 方法进行校验
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(FactoryUtil.FORMAT); // 解码的格式器使用预先定义的配置项
        return options;
    }

    /**
     * 在Connector中可以包含的配置项
     * 后续会通过 helper.validate() 方法进行校验
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BYTE_DELIMITER);
        return options;
    }

    /**
     * 创建并返回 DynamicTableSource
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        // 使用提供的工具类或实现你自己的逻辑进行校验
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // 校验所有的配置项
        helper.validate();

        // 获取校验完的配置项
        final ReadableConfig options = helper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final int port = options.get(PORT);
        final byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);

        // 创建并返回动态表 source
        return new SocketDynamicTableSource(hostname, port, byteDelimiter);
    }
}