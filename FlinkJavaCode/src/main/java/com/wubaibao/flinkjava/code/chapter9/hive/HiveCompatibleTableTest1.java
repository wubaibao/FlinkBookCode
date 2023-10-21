package com.wubaibao.flinkjava.code.chapter9.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Flink SQL 连接Hive中已有的表 ，并进行读写
 */
public class HiveCompatibleTableTest1 {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";//catalog name
        String defaultDatabase = "default";//default database
        String hiveConfDir     = "D:\\idea_space\\MyFlinkCode\\hiveconf";//Hive配置文件目录，Flink读取Hive元数据信息需要

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        //将 HiveCatalog 设置为当前会话的 catalog
        tableEnv.useCatalog("myhive");

        //查询Hive中的表
        tableEnv.executeSql("show tables").print();

        //向Hive中的表插入数据
        tableEnv.executeSql("insert into hive_tbl values (4,'ml',21),(5,'t1',22),(6,'gb',23)");

        //查询Hive中表的数据
        tableEnv.executeSql("select * from hive_tbl").print();
    }
}
