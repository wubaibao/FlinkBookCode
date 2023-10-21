package com.wubaibao.flinkscala.code.test

import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

/**
 * 测试学员 FlinkSQL 支持 Over函数
 *
 * CREATE TABLE mytable (
 * id INT,
 * name VARCHAR(50),
 * age int,
 * event_time TIMESTAMP(3)
 * );
 *
 * INSERT INTO mytable (id, name,age, event_time) VALUES
 * (1,'zs',18,'2023-03-24 10:00:00.123'),
 * (2,'ls',19,'2023-03-24 10:01:00.456'),
 * (3,'ww',20,'2023-03-24 10:02:30.789'),
 * (4,'ml',21,'2023-03-24 10:03:20.234'),
 * (5,'tq',22,'2023-03-24 10:04:40.567');
 *
 *
 * order by 需要指定watermark列才可以
 */
object TestOverFun {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tblEnv: TableEnvironment = TableEnvironment.create(settings)

    tblEnv.executeSql(
      """
        |CREATE TABLE mytable (
        |  id INT,
        |  name STRING,
        |  age INT,
        |  event_time TIMESTAMP(3),
        |  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://192.168.179.5:3306/mydb?useSSL=false',
        |   'username' = 'root',
        |   'password' = '123456',
        |   'table-name' = 'mytable'
        |);
        |""".stripMargin)

    val table: Table = tblEnv.sqlQuery("select id,name,age ,row_number() over(partition by 1 order by event_time ) from mytable")
    table.execute().print()

  }

}
