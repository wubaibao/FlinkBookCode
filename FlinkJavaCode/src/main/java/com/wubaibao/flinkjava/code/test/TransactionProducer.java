package com.wubaibao.flinkjava.code.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionProducer {
    public static void main(String[] args) throws InterruptedException {
        //配置
        Properties pros = new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //开启事务必须开启幂等性
//        pros.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //使用事务，必须指定事务ID
        pros.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "first_producer01");


        //创建生产者（传入配置信息）
        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);

        //初始化事务
        producer.initTransactions();
        //开始事务
        producer.beginTransaction();
        for (int i = 0; i < 100; i++) {
          Thread.sleep(1000);
            producer.send(new ProducerRecord<String, String>("testtopic", "first02 " + i));
            System.out.println("生产一条数据");
        }

        producer.abortTransaction();
        //Thread.sleep(10000);


        //producer.commitTransaction();
        //回滚
//        producer.abortTransaction();
        producer.close();
        //消费者发送消息


    }
}
