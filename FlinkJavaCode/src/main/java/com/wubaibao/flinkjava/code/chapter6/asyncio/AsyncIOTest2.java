package com.wubaibao.flinkjava.code.chapter6.asyncio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 实现Flink异步IO方式二:线程池模拟异步客户端
 * 案例：读取MySQL中的数据
 */
public class AsyncIOTest2 {
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
//        AsyncDataStream.unorderedWait(idDS, new AsyncDatabaseRequest2(), 5000, TimeUnit.MILLISECONDS, 10)
//                .print();
        AsyncDataStream.orderedWait(idDS, new AsyncDatabaseRequest2(), 5000, TimeUnit.MILLISECONDS, 10)
                .print();

        env.execute();

    }
}

class AsyncDatabaseRequest2 extends RichAsyncFunction<Integer, String> {

    //准备线程池对象
    ExecutorService executorService = null;

    //初始化资源，这里主要是初始化线程池
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池,
        // 第一个参数是线程池中线程的数量，
        // 第二个参数是线程池中线程的最大数量，
        // 第三个参数是线程池中线程空闲的时间，
        // 第四个参数是线程池中线程空闲时间的单位，
        // 第五个参数是线程池中的任务队列
        executorService = new ThreadPoolExecutor(10,
                10, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
        //提交异步任务到线程池中
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    /**
                     * 以下两个方法不能设置在open方法中，因为多线程共用数据库连接和pst对象，这样会导致线程不安全
                     */
                    Connection conn = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false",
                            "root", "123456");
                    PreparedStatement pst = conn.prepareStatement("select id,name,age from async_tbl where id = ?");
                    //设置参数
                    pst.setInt(1, input);
                    //执行查询并获取结果
                    ResultSet resultSet = pst.executeQuery();
                    //遍历结果集
                    while (resultSet != null && resultSet.next()) {
                        //获取数据
                        int id = resultSet.getInt("id");
                        String name = resultSet.getString("name");
                        int age = resultSet.getInt("age");
                        //返回结果
                        resultFuture.complete(Arrays.asList("id="+id+",name="+name+",age="+age));
                    }

                    //关闭资源
                    pst.close();
                    conn.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
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
        //关闭线程池
        executorService.shutdown();
    }
}