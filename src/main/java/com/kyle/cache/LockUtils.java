package com.kyle.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import javax.annotation.Resource;
import java.util.*;

/**
 * 分布式锁工具类
 *
 * @author: carroll
 * @date 2019/3/25
 * Copyright @https://github.com/carroll0911. 
 */
@Component
public class LockUtils {
    private static Logger log = LoggerFactory.getLogger(LockUtils.class);
    private static final String LOCK_KEY_PREFIX = "LOCK";
    private static final String KEY_SEPERATOR = "#";

    private static final ThreadLocal<Map<String, String>> lockIds = new ThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String, String> initialValue() {
            return new HashMap<>(16);
        }
    };

    private static final ThreadLocal<Map<String, Long>> lockTimes = new ThreadLocal<Map<String, Long>>() {
        @Override
        protected Map<String, Long> initialValue() {
            return new HashMap<>(16);
        }
    };
    @Resource(
            name = "cacheRedisTemplate"
    )
    private RedisTemplate redisTemplate;
    @Autowired
    private LockConfig lockConfig;
    @Autowired
    private CacheRedisConfig cacheRedisConfig;

    public static final String UNLOCK_LUA;

    static {
        StringBuilder sb = new StringBuilder();
        sb.append("if redis.call(\"get\",KEYS[1]) == ARGV[1] ");
        sb.append("then ");
        sb.append("    return redis.call(\"del\",KEYS[1]) ");
        sb.append("else ");
        sb.append("    return 0 ");
        sb.append("end ");
        UNLOCK_LUA = sb.toString();
    }

    /**
     * 获取锁
     *
     * @param key         key
     * @param expire      锁超时时间
     * @param retryTimes  重试次数
     * @param sleepMillis 重试间隔时间
     * @return
     */
    public boolean lock(String key, long expire, int retryTimes, long sleepMillis) {
        boolean result = setRedis(key, expire);
        // 如果获取锁失败，按照传入的重试次数进行重试
        while (!result && retryTimes-- > 0) {
            try {
                log.debug("lock [{}] failed, retrying...{}", key, retryTimes);
                if (Thread.interrupted()) {
                    log.error("Thread interputed");
                    break;
                }
                Thread.sleep(sleepMillis);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            result = setRedis(key, expire);
        }
        return result;
    }

    public boolean lock(String key, long expire) {
        return this.lock(key, expire, lockConfig.getRetryTimes(), lockConfig.getSleepMillis());
    }

    public boolean lock(String key) {
        return this.lock(key, lockConfig.getDefaultExpireMs(), lockConfig.getRetryTimes(), lockConfig.getSleepMillis());
    }

    private boolean setRedis(String key, long expire) {
        try {
            String uuid = UUID.randomUUID().toString();

            String result = (String) redisTemplate.execute((RedisCallback<String>) connection -> {
                JedisCommands commands = (JedisCommands) connection.getNativeConnection();
                return commands.set(getKey(key), uuid, "NX", "PX", expire <= 0 ? lockConfig.getDefaultExpireMs() : expire);
            });
            boolean lockRes = !StringUtils.isEmpty(result);
            if (lockRes) {
                String oldKey = lockIds.get().get(key);
                if (!StringUtils.isEmpty(oldKey)) {
                    log.warn("锁未被正常释放:{}-{}", Thread.currentThread().getName(), key);
                }
                lockIds.get().put(key, uuid);
                lockTimes.get().put(key, System.currentTimeMillis());
            }
            return lockRes;
        } catch (Exception e) {
            log.error("set redis occured an exception", e);
        }
        return false;
    }

    /**
     * 释放锁
     *
     * @param key
     */
    public boolean releaseLock(String key) {
        // 释放锁的时候，有可能因为持锁之后方法执行时间大于锁的有效期，此时有可能已经被另外一个线程持有锁，所以不能直接删除
        try {
            List<String> keys = new ArrayList<String>();
            keys.add(getKey(key));
            List<String> args = new ArrayList<String>();
            args.add(lockIds.get().get(key));

            // 使用lua脚本删除redis中匹配value的key，可以避免由于方法执行时间过长而redis锁自动过期失效的时候误删其他线程的锁
            // spring自带的执行脚本方法中，集群模式直接抛出不支持执行脚本的异常，所以只能拿到原redis的connection来执行脚本

            Long result = (Long) redisTemplate.execute((RedisCallback<Long>) connection -> {
                Object nativeConnection = connection.getNativeConnection();
                // 集群模式和单机模式虽然执行脚本的方法一样，但是没有共同的接口，所以只能分开执行
                // 集群模式
                if (nativeConnection instanceof JedisCluster) {
                    return (Long) ((JedisCluster) nativeConnection).eval(UNLOCK_LUA, keys, args);
                }

                // 单机模式
                else if (nativeConnection instanceof Jedis) {
                    return (Long) ((Jedis) nativeConnection).eval(UNLOCK_LUA, keys, args);
                }
                return 0L;
            });
            Long lockTime = lockTimes.get().get(key);
            if (lockTime != null) {
                log.info("锁占用时长:{}-{}-{}", Thread.currentThread().getName(), key, System.currentTimeMillis() - lockTime);
            }
            boolean release = result != null && result > 0;
            if (release) {
                lockIds.get().remove(key);
            }
        } catch (Exception e) {
            log.error("release lock occured an exception", e);
        }
        return false;
    }

    private String getKey(String key) {
        return String.format("%s:%s%s%s", cacheRedisConfig.getCacheName(), LOCK_KEY_PREFIX, KEY_SEPERATOR, key);
    }
}
