package com.wubaibao.flinkscala.code.test

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object InsertIntoFile2 {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tblEnv: TableEnvironment = TableEnvironment.create(settings)
    //Flink SQL 中设置checkpoint的间隔
    tblEnv.getConfig.getConfiguration.setLong("execution.checkpointing.interval", 1000L)

    tblEnv.executeSql(
      """
        |create table kafka_tp(
        |   id int,
        |   name string,
        |   age int
        |)with(
        | 'connector' = 'kafka',
        | 'topic' = 'tp',
        | 'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092',
        | 'scan.startup.mode'='latest-offset',
        | 'properties.group.id' = 'my-group-id',
        | 'format' = 'csv'
        |)
        |""".stripMargin)


    tblEnv.executeSql(
      """
        | CREATE  TABLE ttt (
        |  id int,
        |  name STRING,
        |  age int
        |)WITH (
        |  'connector' = 'filesystem',
        |  'path' = './ttxx',
        |  'sink.rolling-policy.check-interval' = '1s', -- 每隔1s检查一次，是否需要滚动文件
        |  'sink.rolling-policy.rollover-interval' = '10s',-- 滚动文件的时间间隔
        |  'sink.rolling-policy.file-size'= '1MB', -- 滚动文件的大小
        |  'sink.parallelism' = '1',-- 并行度
        |  'format' = 'csv')
        |""".stripMargin)


    tblEnv.executeSql(
      """
        | INSERT INTO ttt (id,name,age)
        | SELECT id,name,age FROM kafka_tp
        |""".stripMargin)

  }

}
