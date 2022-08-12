package com.example.mysqlcanalredis.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName RedisUtil
 * @Description TODO
 * @Author 听秋
 * @Date 2022/4/21 20:22
 * @Version 1.0
 **/
public class RedisUtil {
    private static String ip = "127.0.0.1";
    private static int port = 6379;
    private static int timeout = 10000;
    private static JedisPool pool = null;


    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1024);//最大连接数
        config.setMaxIdle(200);//最大空闲实例数
        config.setMaxWaitMillis(10000);//等连接池给连接的最大时间，毫秒
        config.setTestOnBorrow(true);//borrow一个实例的时候，是否提前vaildate操作

        pool = new JedisPool(config, ip, port, timeout);

    }

    //得到redis连接
    public static Jedis getJedis() {
        if (pool != null) {
            return pool.getResource();
        } else {
            return null;
        }
    }

    //关闭redis连接
    public static void close(final Jedis redis) {
        if (redis != null) {
            redis.close();
        }
    }

    public static boolean existKey(String key) {
        return getJedis().exists(key);
    }

    public static void delKey(String key) {
        getJedis().del(key);
    }

    public static String stringGet(String key) {
        return getJedis().get(key);
    }

    public static String stringSet(String key, String value) {
        return getJedis().set(key, value);
    }

    public static String stringSet(String key, String value, long time) {
        return getJedis().set(key, value, null, null, time);
    }

    public static void hashSet(String key, String field, String value) {
        getJedis().hset(key, field, value);
    }

}

