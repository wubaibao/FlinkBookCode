package com.wubaibao.flinkscala.code.chapter9.hive

import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}

/**
 * Flink SQL 使用Hive方言创建Hive表，并进行读写
 */
object HiveCompatibleTableTest2 {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //设置Hive 方言
    tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE)

    //SQL 方式创建 Hive Catalog
    tableEnv.executeSql("CREATE CATALOG myhive WITH (" +
      "  'type'='hive'," +//指定Catalog类型为hive
      "  'default-database'='default'," +//指定默认数据库
      "  'hive-conf-dir'='D:\\idea_space\\MyFlinkCode\\hiveconf'" +//指定Hive配置文件目录，Flink读取Hive元数据信息需要
      ")")

    //将 HiveCatalog 设置为当前会话的 catalog
    tableEnv.useCatalog("myhive")

    //创建Hive表
    tableEnv.executeSql("create table if not exists flink_hive_tbl (id int ,name string,age int) " +
      "row format delimited fields terminated by '\t'")

    //插入数据
    tableEnv.executeSql("insert into flink_hive_tbl values (1,'zs',18),(2,'ls',19),(3,'ww',20)")

    //查询数据
    tableEnv.executeSql("select * from flink_hive_tbl").print()

  }

}
