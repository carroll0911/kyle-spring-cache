package com.carroll.cache;

import org.springframework.data.redis.cache.RedisCachePrefix;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author: carroll
 * @date 2019/3/5
 * Copyright @https://github.com/carroll0911. 
 */
public class ExtendRedisCachePrefix implements RedisCachePrefix {
    private final RedisSerializer serializer;
    private final String delimiter;
    private final String defaultCacheName;

    public ExtendRedisCachePrefix(String defaultCacheName) {
        this(":", defaultCacheName);
    }

    public ExtendRedisCachePrefix(String delimiter, String defaultCacheName) {
        this.serializer = new StringRedisSerializer();
        this.delimiter = delimiter;
        this.defaultCacheName = defaultCacheName;
    }

    @Override
    public byte[] prefix(String cacheName) {
        return this.serializer.serialize(String.format("%s%s%s%s", defaultCacheName, delimiter, cacheName, delimiter));
    }
}
