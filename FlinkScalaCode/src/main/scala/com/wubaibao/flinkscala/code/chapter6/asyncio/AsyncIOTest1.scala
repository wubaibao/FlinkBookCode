package com.wubaibao.flinkscala.code.chapter6.asyncio

import io.vertx.core.{AsyncResult, Handler, VertxOptions}
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.{ResultSet, SQLConnection}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter


/**
 * 实现Flink异步IO方式一:使用 Vert.x 实现异步 IO
 * 案例：读取MySQL中的数据
 */
object AsyncIOTest1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //为了测试效果，这里设置并行度为1
    env.setParallelism(1)

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //准备数据流
    val idDS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    AsyncDataStream.unorderedWait(idDS, new AsyncDatabaseRequest1(), 5000, java.util.concurrent.TimeUnit.MILLISECONDS, 10)
        .print()

    env.execute()
  }

}

class AsyncDatabaseRequest1 extends RichAsyncFunction[Int,String]() {

  //定义JDBCClient对象
  var mysqlClient: JDBCClient = null

  //初始化资源,连接MySQL
  override def open(parameters: Configuration): Unit = {
    //创建连接MySQL的配置信息
    val config: JsonObject = new JsonObject()
      .put("url", "jdbc:mysql://node2:3306/mydb?useSSL=false")
      .put("driver_class", "com.mysql.jdbc.Driver")
      .put("user", "root")
      .put("password", "123456")

    //创建VertxOptions对象
    val vo = new VertxOptions()
    //设置Vertx要使用的事件循环线程数
    vo.setEventLoopPoolSize(10)
    //设置Vertx要使用的最大工作线程数
    vo.setWorkerPoolSize(20)

    //创建Vertx对象
    val vertx = io.vertx.core.Vertx.vertx(vo)
    //创建 JDBCClient 共享对象，多个Vertx客户端可以共享一个JDBCClient实例
    mysqlClient = JDBCClient.createShared(vertx, config)
  }

  //实现异步IO的方法,第一个参数是输入，第二个参数是异步IO返回的结果
  override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {
    //获取MySQL连接
    mysqlClient.getConnection(new Handler[AsyncResult[SQLConnection]] {
      override def handle(sqlConnectionAsyncResult: AsyncResult[SQLConnection]): Unit = {
        if(!sqlConnectionAsyncResult.failed()){
          //获取连接
          val connection : SQLConnection = sqlConnectionAsyncResult.result()

          //执行查询
          connection.query("select id,name,age from async_tbl where id = " + input,new Handler[AsyncResult[ResultSet]] {
            override def handle(resultSetAsyncResult: AsyncResult[ResultSet]): Unit = {
              if(!resultSetAsyncResult.failed()){
                //获取查询结果
                val resultSet: ResultSet = resultSetAsyncResult.result()
                resultSet.getRows().asScala.foreach(row=>{
                  //返回结果
                  resultFuture.complete(List(row.encode()))
                })
              }
            }
          })
        }
      }
    })

  }

  /**
   * 异步IO超时处理逻辑，主要避免程序出错。参数如下:
   * @param input 输入数据
   * @param resultFuture 异步IO返回的结果
   */
  override def timeout(input: Int, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(List("异步IO超时！！！"))
  }

  //关闭资源
  override def close(): Unit = {
    mysqlClient.close() //关闭连接
  }
}
