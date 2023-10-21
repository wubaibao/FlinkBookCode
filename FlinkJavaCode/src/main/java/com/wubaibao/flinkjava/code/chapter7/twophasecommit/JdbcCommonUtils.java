package com.wubaibao.flinkjava.code.chapter7.twophasecommit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 连接MySQL - JDBC工具类
 */
public class JdbcCommonUtils{
    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://node2:3306/mydb?useSSL=false";
    private static String username = "root";
    private static String password = "123456";

    private static Connection conn ;

    /**
     * 获取数据库连接对象
     */
    public  Connection getConnect() {
        try {
            if(conn ==null){
                Class.forName(driver);
                conn = DriverManager.getConnection(url, username, password);
                //设置手动提交事务
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return conn;
    }

    /**
     * 提交事务
     */
    public  void commit() {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }

    /**
     * 回滚事务
     */
    public  void rollback() {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
