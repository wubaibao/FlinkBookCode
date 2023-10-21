package com.wubaibao.flinkscala.code.test

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object InsertIntoFile {
  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //.inBatchMode()
      .build()

    val tblEnv: TableEnvironment = TableEnvironment.create(settings)

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
        | 'scan.startup.mode'='earliest-offset',
        | 'properties.group.id' = 'my-group-id',
        | 'format' = 'csv'
        |)
        |""".stripMargin)

    tblEnv.executeSql("select * from kafka_tp").print()


    //    val table: Table = tblEnv.sqlQuery("select id,name,age ,row_number() over(partition by name order by id ) from myperson")
    //    table.execute().print()

    //    val result: TableResult = tblEnv.executeSql("SELECT count(id) from myperson1")
    //    result.print()

    //    tblEnv.executeSql(
    //      """
    //        |CREATE TABLE myperson2 (
    //        |  id INT,
    //        |  name STRING,
    //        |  age INT,
    //        |  PRIMARY KEY (id) NOT ENFORCED
    //        |) WITH (
    //        |   'connector' = 'jdbc',
    //        |   'url' = 'jdbc:mysql://192.168.179.5:3306/test_ch?useSSL=false',
    //        |   'username' = 'root',
    //        |   'password' = '123456',
    //        |   'table-name' = 'person2'
    //        |);
    //        |""".stripMargin)


    //    val result: TableResult = tblEnv.executeSql(
    //      """
    //        | INSERT INTO myperson2 (id,name,age)
    //        | SELECT id,name,age FROM myperson1
    //        |""".stripMargin)

    //    result.print()
  }

}
