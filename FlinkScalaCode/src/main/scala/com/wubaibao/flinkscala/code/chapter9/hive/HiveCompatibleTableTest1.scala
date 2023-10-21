package com.wubaibao.flinkscala.code.chapter9.hive

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * Flink SQL 连接Hive中已有的表 ，并进行读写
 */
object HiveCompatibleTableTest1 {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    val name            = "myhive" //catalog name
    val defaultDatabase = "default" //default database
    val hiveConfDir     = "D:\\idea_space\\MyFlinkCode\\hiveconf" //Hive配置文件目录，Flink读取Hive元数据信息需要

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tableEnv.registerCatalog("myhive", hive)

    //将 HiveCatalog 设置为当前会话的 catalog
    tableEnv.useCatalog("myhive")

    //查询Hive中的表
    tableEnv.executeSql("show tables").print()

    //向Hive中的表插入数据
    tableEnv.executeSql("insert into hive_tbl values (4,'ml',21),(5,'t1',22),(6,'gb',23)")

    //查询Hive中表的数据
    tableEnv.executeSql("select * from hive_tbl").print()
  }

}
