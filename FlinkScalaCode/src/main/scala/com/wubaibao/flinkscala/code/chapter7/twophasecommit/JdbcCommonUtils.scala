package com.wubaibao.flinkscala.code.chapter7.twophasecommit

import java.sql.{Connection, DriverManager, SQLException}

/**
 * 连接Mysql - JDBC工具类
 */
case class JdbcCommonUtils() {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://node2:3306/mydb?useSSL=false"
  val username = "root"
  val password = "123456"

  private var conn: Connection = _
  /**
   * 获取数据库连接对象
   */
  def getConnect: Connection = {
    if (conn == null) {
      try {
        Class.forName(driver)
        conn = DriverManager.getConnection(url, username, password)
        //设置手动提交事务
        conn.setAutoCommit(false)
      } catch {
        case e: SQLException =>
          throw new RuntimeException(e)
        case e: ClassNotFoundException =>
          throw new RuntimeException(e)
      }
    }
    conn
  }

  /**
   * 提交事务
   */
  def commit(): Unit = {
    if (conn != null) {
      try {
        conn.commit()
      } catch {
        case e: SQLException =>
          throw new RuntimeException(e)
      }
    }
  }

  /**
   * 回滚事务
   */
  def rollback(): Unit = {
    if (conn != null) {
      try {
        conn.rollback()
      } catch {
        case e: SQLException =>
          throw new RuntimeException(e)
      }
    }
  }

  /**
   * 关闭连接
   */
  def close(): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: SQLException =>
          throw new RuntimeException(e)
      }
    }
  }
}