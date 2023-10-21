package com.wubaibao.flinkjava.code.chapter9.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink 实时读取 Hive分区表数据
 * 案例：实时读取Hive分区表数据
 */
public class FlinkRTReadHive {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //SQL 方式创建 Hive Catalog
        tableEnv.executeSql("CREATE CATALOG myhive WITH (" +
                "  'type'='hive'," +//指定Catalog类型为hive
                "  'default-database'='default'," +//指定默认数据库
                "  'hive-conf-dir'='D:\\idea_space\\MyFlinkCode\\hiveconf'" +//指定Hive配置文件目录，Flink读取Hive元数据信息需要
                ")");

        //将 HiveCatalog 设置为当前会话的 catalog
        tableEnv.useCatalog("myhive");

        //Flink SQL 方式实时读取Hive分区表数据
        tableEnv.executeSql("select * from rt_hive_tbl " +
                "/*+ OPTIONS(" +
                "   'streaming-source.enable'='true', " +
                "   'streaming-source.monitor-interval' = '2 second'," +
                "   'streaming-source.consume-start-offset'='1970-01-01 08:00:00'" +
                "   ) " +
                "*/").print();
    }
}
