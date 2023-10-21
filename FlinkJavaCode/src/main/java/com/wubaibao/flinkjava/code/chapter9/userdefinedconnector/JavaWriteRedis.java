package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Java连接Redis 向Redis写入数据
 */
public class JavaWriteRedis {
    public static void main(String[] args) {
        /**
         * 创建Jedis 连接池
         */
        JedisPoolConfig config = new JedisPoolConfig();
        // 指定最大空闲连接为10个
        config.setMaxIdle(10);
        // 最小空闲连接5个
        config.setMinIdle(5);
        // 最大等待时间为3000毫秒
        config.setMaxWaitMillis(3000);
        // 最大连接数为50
        config.setMaxTotal(50);


        //包含用户名，密码，连接地址，端口号，数据库号
        JedisPool  jedisPool = new JedisPool(config, "node4", 6379);

        Jedis jedis = jedisPool.getResource();
        jedis.select(1);
        jedis.hset("goods","iphone14","10000");







    }
}
