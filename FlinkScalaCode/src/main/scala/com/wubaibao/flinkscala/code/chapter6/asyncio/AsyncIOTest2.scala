package com.wubaibao.flinkscala.code.chapter6.asyncio

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.concurrent.{ExecutorService, Executors, ThreadPoolExecutor}

/**
 * 实现Flink异步IO方式二:线程池模拟异步客户端
 * 案例：读取MySQL中的数据
 */
object AsyncIOTest2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //为了测试效果，这里设置并行度为1
    env.setParallelism(1)

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //准备数据流
    val idDS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    /**
     * 使用异步IO，参数解释如下：
     *  第一个参数是输入数据流，
     *  第二个参数是异步IO的实现类，
     *  第三个参数是用于完成异步操作超时时间，
     *  第四个参数是超时时间单位，
     *  第五个参数可以触发的最大异步i/o操作数
     */
    AsyncDataStream.unorderedWait(idDS, new AsyncDatabaseRequest2(), 5000, java.util.concurrent.TimeUnit.MILLISECONDS, 10)
      .print()

    env.execute()
  }

}

class AsyncDatabaseRequest2 extends RichAsyncFunction[Int,String]() {
  //准备线程池对象
  var executorService: ExecutorService = null


  //初始化资源,准备线程池
  override def open(parameters: Configuration): Unit = {
    //初始化线程池,
    // 第一个参数是线程池中线程的数量，
    // 第二个参数是线程池中线程的最大数量，
    // 第三个参数是线程池中线程空闲的时间，第四个参数是线程池中线程空闲时间的单位，第五个参数是线程池中的任务队列
    executorService = new ThreadPoolExecutor(10,10,0L,
      java.util.concurrent.TimeUnit.MILLISECONDS,
      new java.util.concurrent.LinkedBlockingQueue[Runnable]())
  }

  //多线程方式处理数据
  override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {
    //使用线程池执行异步任务
    executorService.submit(new Runnable {
      override def run(): Unit = {
        /**
         * 以下两个方法不能设置在open方法中，因为多线程共用数据库连接和pst对象，这样会导致线程不安全
         */
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false", "root", "123456")
        val pst: PreparedStatement = conn.prepareStatement("select id,name,age from async_tbl where id = ?")

        //设置参数
        pst.setInt(1, input)
        //执行查询并获取结果
        val rs = pst.executeQuery()
        while(rs!=null && rs.next()){
          val id: Int = rs.getInt("id")
          val name: String = rs.getString("name")
          val age: Int = rs.getInt("age")
          //将结果返回给Flink
          resultFuture.complete(List("id = "+id+",name = "+name+",age = "+age))
        }

        //关闭资源
        pst.close();
        conn.close();

      }
    })
  }

  /**
   * 异步IO超时处理逻辑，主要避免程序出错。参数如下:
   * 第一个参数是输入数据
   * 第二个参数是异步IO返回的结果
   */
  override def timeout(input: Int, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(List("异步IO超时了！！！"))
  }

  //关闭资源
  override def close(): Unit = {
    //关闭线程池
    executorService.shutdown()
  }
}
