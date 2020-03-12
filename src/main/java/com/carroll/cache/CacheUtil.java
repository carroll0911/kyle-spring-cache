package com.carroll.cache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

/**
 * @author carroll on 2017/5/19.
 * Cache工具类
 */
@Component
public class CacheUtil {
    @Autowired
    private CacheManager cacheManager;

    private static final String SYS_CACHE = "sysCache";

    /**
     * 获取SYS_CACHE缓存
     * @param key
     * @return
     */
    public Object get(String key) {
        return get(SYS_CACHE, key);
    }

    /**
     * 写入SYS_CACHE缓存
     * @param key
     * @return
     */
    public void put(String key, Object value) {
        put(SYS_CACHE, key, value);
    }

    /**
     * 从SYS_CACHE缓存中移除
     * @param key
     * @return
     */
    public void evict(String key) {
        evict(SYS_CACHE, key);
    }

    /**
     * 获取缓存
     * @param cacheName
     * @param key
     * @return
     */
    public Object get(String cacheName, String key) {
        return getCache(cacheName).get(key);
    }

    /**
     * 写入缓存
     * @param cacheName
     * @param key
     * @param value
     */
    public void put(String cacheName, String key, Object value) {
        getCache(cacheName).put(key,value);
    }

    /**
     * 从缓存中移除
     * @param cacheName
     * @param key
     */
    public void evict(String cacheName, String key) {
        getCache(cacheName).evict(key);
    }

    /**
     * 获得一个Cache，没有则创建一个。
     * @param cacheName
     * @return
     */
    private Cache getCache(String cacheName){
        return cacheManager.getCache(cacheName);
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }
}
