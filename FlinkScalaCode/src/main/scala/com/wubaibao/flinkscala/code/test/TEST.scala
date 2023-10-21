package com.wubaibao.flinkscala.code.test

import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

/**
 * mysql> create table person(id int ,name varchar(255),age int);
 * insert into person values (1,'zs',18),(2,'ls',19),(3,'ww',20);
 *
 */
object TEST {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //.inBatchMode()
      .build()
    val tblEnv: TableEnvironment = TableEnvironment.create(settings)

    tblEnv.executeSql(
      """
        |CREATE TABLE myperson (
        |  id INT,
        |  name STRING,
        |  age INT
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://192.168.179.5:3306/mydb?useSSL=false',
        |   'username' = 'root',
        |   'password' = '123456',
        |   'table-name' = 'person'
        |);
        |""".stripMargin)

    //        val table: Table = tblEnv.sqlQuery("select * from myperson")
    val table: Table = tblEnv.sqlQuery("select id,name,age ,row_number() over(partition by name order by id ) from myperson")
    table.execute().print()

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
