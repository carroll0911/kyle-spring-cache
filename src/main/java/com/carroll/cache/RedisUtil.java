package com.carroll.cache;

/**
 * redicache 工具类
 *
 * @author carroll
 * <p>
 * Copyright @https://github.com/carroll0911. 
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
@Component
public class RedisUtil {
    private static Logger log = LoggerFactory.getLogger(RedisUtil.class);

    @SuppressWarnings("rawtypes")
    @Resource(name = "cacheRedisTemplate")
    private RedisTemplate redisTemplate;
    @Autowired
    private CacheRedisConfig cacheRedisConfig;

    private static final String delimiter = ":";

    /**
     * 批量删除对应的value
     *
     * @param keys
     */
    public void remove(final String prefix, final String... keys) {
        for (String key : keys) {
            remove(dealWithKey(prefix, key));
        }
    }

    public void remove(final String... keys) {
        remove(null, keys);
    }

    /**
     * 批量删除key
     *
     * @param pattern
     */
    public void removePattern(final String pattern) {
        Set<Serializable> keys = redisTemplate.keys(pattern);
        if (!keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }

    /**
     * 删除对应的value
     *
     * @param key
     */
    public void remove(final String prefix, final String key) {
        String newKey = dealWithKey(prefix, key);
        if (redisTemplate.hasKey(newKey)) {
            redisTemplate.delete(newKey);
        }
    }

    public void remove(final String key) {
        remove(null, key);
    }

    /**
     * 判断缓存中是否有对应的value
     *
     * @param key
     * @return
     */
    public boolean exists(final String prefix, final String key) {
        return redisTemplate.hasKey(dealWithKey(prefix, key));
    }

    public boolean exists(final String key) {
        return exists(null, key);
    }

    /**
     * 读取缓存
     *
     * @param key
     * @return
     */
    public Object get(final String prefix, final String key) {
        Object result = null;
        ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
        result = operations.get(dealWithKey(prefix, key));
        return result;
    }

    public Object get(final String key) {
        return get(null, key);
    }

    /**
     * 写入缓存
     *
     * @param key
     * @param value
     * @return
     */
    public boolean set(final String key, Object value) {
        return setWithPrefix(null, key, value);
    }

    public boolean setWithPrefix(final String prefix, final String key, Object value) {
        boolean result = false;
        try {
            ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
            operations.set(dealWithKey(prefix, key), value);
            result = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 写入缓存
     *
     * @param key
     * @param value
     * @param expireTime
     * @param unit
     * @return
     */
    public boolean set(final String key, Object value, Long expireTime, final TimeUnit unit) {
        return set(null, key, value, expireTime, unit);
    }

    public boolean set(final String prefix, final String key, Object value, Long expireTime, final TimeUnit unit) {
        boolean result = false;
        String newKey = dealWithKey(prefix, key);
        try {
            ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
            operations.set(newKey, value);
            redisTemplate.expire(newKey, expireTime, unit);
            result = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 写入缓存
     *
     * @param key
     * @param value
     * @param expireTime
     * @return
     */
    public boolean set(final String key, Object value, Long expireTime) {
        return set(key, value, expireTime, TimeUnit.SECONDS);
    }

    public boolean set(final String prefix, final String key, Object value, Long expireTime) {
        return set(prefix, key, value, expireTime, TimeUnit.SECONDS);
    }

    public boolean setIfAbsent(Object k, Object v) {
        return redisTemplate.opsForValue().setIfAbsent(dealWithKey(null, String.valueOf(k)), v);
    }

    public boolean setIfAbsent(String prefix, Object k, Object v) {
        return redisTemplate.opsForValue().setIfAbsent(dealWithKey(prefix, String.valueOf(k)), v);
    }

    public RedisTemplate getRedisTemplate() {
        return redisTemplate;
    }

    public Boolean expire(Object key, final long timeout, final TimeUnit unit) {
        return redisTemplate.expire(key, timeout, TimeUnit.SECONDS);
    }

    private String dealWithKey(String prefix, String key) {
        return String.format("%s%s%s", StringUtils.isEmpty(prefix) ? cacheRedisConfig.getCacheName() : prefix, delimiter, key);
    }
}
