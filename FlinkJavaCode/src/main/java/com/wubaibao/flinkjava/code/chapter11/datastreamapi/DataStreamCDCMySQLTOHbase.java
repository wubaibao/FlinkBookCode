package com.wubaibao.flinkjava.code.chapter11.datastreamapi;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

/**
 * 通过Flink MySQL CDC 将MySQL数据同步到Hbase中
 * DataStream API 实现
 */
public class DataStreamCDCMySQLTOHbase {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node2")      //设置MySQL hostname
                .port(3306)             //设置MySQL port
                .databaseList("db1")    //设置捕获的数据库
                .tableList("db1.tbl1") //设置捕获的数据表
                .username("root")       //设置登录MySQL用户名
                .password("123456")     //设置登录MySQL密码
                .deserializer(new JsonDebeziumDeserializationSchema()) //设置序列化将SourceRecord 转换成 Json 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(5000);

        //转换数据为 JsonObject 类型
        SingleOutputStreamOperator<JSONObject> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return new JSONObject(value);
                    }
                });

        //将数据写出到HBase
        ds.addSink(new RichSinkFunction<JSONObject>() {

            org.apache.hadoop.hbase.client.Connection conn = null;

            //在Sink 初始化时调用一次,这里创建 HBase连接
            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum","node3,node4,node5");
                conf.set("hbase.zookeeper.property.clientPort","2181");
                //创建连接
                conn = ConnectionFactory.createConnection(conf);
            }

            //Sink数据时，每条数据插入时调用一次
            @Override
            public void invoke(JSONObject currentOne, Context context) throws Exception {
                /**
                 * 增加的数据
                 * {"before":null,"after":{"id":3,"name":"ww","age":20},...,"op":"c","ts_ms":1696941931728,"transaction":null}
                 *
                 * 修改的数据
                 * {"before":{"id":2,"name":"ls","age":19},"after":{"id":2,"name":"lisi","age":20},...,"op":"u","ts_ms":1696941937208,"transaction":null}
                 *
                 * 删除的数据
                 * {"before":{"id":3,"name":"ww","age":20},"after":null,...,"op":"d","ts_ms":1696941941399,"transaction":null}
                 */
                String op = currentOne.getString("op");
                //增加或者更新
                if("c".equals(op)||"u".equals(op)){
                    JSONObject insertJsonObj = currentOne.getJSONObject("after");
                    //准备rowkey
                    String rowKey = insertJsonObj.getString("id");

                    //获取列
                    String id = rowKey;
                    String name = insertJsonObj.getString("name");
                    String age = insertJsonObj.getString("age");

                    //获取表对象
                    Table table = conn.getTable(TableName.valueOf("tbl_cdc"));

                    //创建Put对象
                    Put p = new Put(Bytes.toBytes(rowKey));
                    //添加列
                    p.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("id"),Bytes.toBytes(id));
                    p.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes(name));
                    p.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(age));

                    //插入数据
                    table.put(p);

                    //关闭表对象
                    table.close();
                }

                //删除
                if("d".equals(op)){
                    JSONObject insertJsonObj = currentOne.getJSONObject("before");
                    //准备rowkey
                    String rowKey = insertJsonObj.getString("id");

                    Table table = conn.getTable(TableName.valueOf("tbl_cdc"));

                    Delete del = new Delete(Bytes.toBytes(rowKey));
                    del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("id"));
                    del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
                    del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
                    // 删除指定列族
//                    del.addFamily(Bytes.toBytes("cf1"));

                    // 删除rowkey数据
                    table.delete(del);

                    //关闭表对象
                    table.close();
                }

            }

            //在Sink 关闭时调用一次，这里关闭HBase连接
            @Override
            public void close() throws Exception {
                //关闭连接
                conn.close();
            }

        });

        env.execute();

    }

}
