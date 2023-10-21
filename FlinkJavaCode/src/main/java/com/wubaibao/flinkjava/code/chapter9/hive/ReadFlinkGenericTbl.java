package com.wubaibao.flinkjava.code.chapter9.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL 读取Hive中的普通表
 */
public class ReadFlinkGenericTbl {
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

        //通过SQL 查询表中数据
        tableEnv.executeSql("select * from flink_kafka_tbl " +
                "/*+ OPTIONS('scan.startup.mode'='earliest-offset') */").print();
    }
}
