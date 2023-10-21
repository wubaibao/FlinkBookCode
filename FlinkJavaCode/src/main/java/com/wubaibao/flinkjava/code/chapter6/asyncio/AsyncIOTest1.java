package com.wubaibao.flinkjava.code.chapter6.asyncio;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *  实现Flink异步IO方式一:使用 Vert.x 实现异步 IO
 *  案例：读取MySQL中的数据
 */
public class AsyncIOTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为了测试效果，这里设置并行度为1
        env.setParallelism(1);
        //准备数据流
        DataStreamSource<Integer> idDS = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        /**
         * 使用异步IO，参数解释如下：
         *  第一个参数是输入数据流，
         *  第二个参数是异步IO的实现类，
         *  第三个参数是用于完成异步操作超时时间，
         *  第四个参数是超时时间单位，
         *  第五个参数可以触发的最大异步i/o操作数
         */
        AsyncDataStream.unorderedWait(idDS, new AsyncDatabaseRequest1(), 5000, TimeUnit.MILLISECONDS, 10)
                .print();

        env.execute();

    }

}

class AsyncDatabaseRequest1 extends RichAsyncFunction<Integer, String> {
    //定义JDBCClient共享对象
    JDBCClient mysqlClient = null;

    //初始化资源，连接Mysql
    @Override
    public void open(Configuration parameters) throws Exception {
        //创建连接mysql配置对象
        JsonObject config = new JsonObject()
                .put("url", "jdbc:mysql://node2:3306/mydb?useSSL=false")
                .put("driver_class", "com.mysql.jdbc.Driver")
                .put("user", "root")
                .put("password", "123456");

        //创建VertxOptions对象
        VertxOptions vo = new VertxOptions();
        //设置Vertx要使用的事件循环线程数
        vo.setEventLoopPoolSize(10);
        //设置Vertx要使用的最大工作线程数
        vo.setWorkerPoolSize(20);

        //创建Vertx对象
        Vertx vertx = Vertx.vertx(vo);

        //创建JDBCClient共享对象，多个Vertx 客户端可以共享一个JDBCClient对象
        mysqlClient = JDBCClient.createShared(vertx, config);
    }

    //实现异步IO的方法，第一个参数是输入，第二个参数是异步IO返回的结果
    @Override
    public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) {
        mysqlClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                if (sqlConnectionAsyncResult.failed()) {
                    System.out.println("获取连接失败：" + sqlConnectionAsyncResult.cause().getMessage());
                    return;
                }

                //获取连接
                SQLConnection connection = sqlConnectionAsyncResult.result();

                //执行查询
                connection.query("select id,name,age from async_tbl where id = " + input,
                        new Handler<AsyncResult<io.vertx.ext.sql.ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<io.vertx.ext.sql.ResultSet> resultSetAsyncResult) {
                        if (resultSetAsyncResult.failed()) {
                            System.out.println("查询失败：" + resultSetAsyncResult.cause().getMessage());
                            return;
                        }

                        //获取查询结果
                        io.vertx.ext.sql.ResultSet resultSet = resultSetAsyncResult.result();

                        //打印查询的结果
                        //将查询结果返回给Flink
                        resultSet.getRows().forEach(row -> {
                            resultFuture.complete(Collections.singletonList(row.encode()));
                        });
                    }
                });
            }
        });
    }


    /**
     * 异步IO超时处理逻辑，主要避免程序出错。参数如下:
     * 第一个参数是输入数据
     * 第二个参数是异步IO返回的结果
     */
    @Override
    public void timeout(Integer input, ResultFuture<String> resultFuture) throws Exception {
        resultFuture.complete(Collections.singletonList("异步IO超时！！！"));
    }

    //关闭资源
    @Override
    public void close() throws Exception {
        mysqlClient.close();
    }
}
