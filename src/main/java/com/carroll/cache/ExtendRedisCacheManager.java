package com.carroll.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.data.redis.cache.RedisCache;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCachePrefix;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.util.StringUtils;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

/**
 * 扩展的RedisCacheManage
 *
 * @author carroll
 * @Date 2017-05-11 18:27
 **/
public class ExtendRedisCacheManager extends RedisCacheManager {
    private static Logger log = LoggerFactory.getLogger(RedisUtil.class);

    private static final ScriptEngine SCRIPT_ENGINE = new ScriptEngineManager().getEngineByName("JavaScript");

    private static final Pattern PATTERN = Pattern.compile("[+\\-*/%]");

    private char separator = '#';

    private String defaultCacheName;

    private long defaultExpiration = 0;

    public ExtendRedisCacheManager(RedisOperations redisOperations) {
        this(redisOperations, Collections.<String>emptyList());
    }

    public ExtendRedisCacheManager(RedisOperations redisOperations, Collection<String> cacheNames) {
        super(redisOperations, cacheNames);
    }

    @Override
    public Cache getCache(String name) {
        log.debug("start getCache");
        Long expiration = defaultExpiration;
        String cacheName = defaultCacheName;
        int index = name.lastIndexOf(getSeparator());
        if(index > -1){
            expiration = getExpiration(name, index);
            if (expiration == null || expiration < 0) {
                expiration = defaultExpiration;
            }
            super.setDefaultExpiration(expiration);
            cacheName = name.substring(0, index);
            if(StringUtils.isEmpty(cacheName)||StringUtils.isEmpty(cacheName.trim())){
                cacheName = defaultCacheName;
            }
            cacheName=cacheName+name.substring(index);
        } else if(!StringUtils.isEmpty(name)) {
            cacheName = name;
        }
        // try to get cache by name
        RedisCache cache = (RedisCache) super.getCache(cacheName);
        log.debug("end getCache");
        if (cache != null) {
            return cache;
        }

        CustomRedisCache redisCache = new CustomRedisCache(cacheName, (isUsePrefix() ? getCachePrefix().prefix(cacheName) : null), getRedisOperations(), expiration);
        log.debug("end getCache2");
        return redisCache;
    }


    public char getSeparator() {
        return separator;
    }

    /**
     * Char that separates cache name and expiration time, default: #.
     *
     * @param separator
     */
    public void setSeparator(char separator) {
        this.separator = separator;
    }

    private Long getExpiration(final String name, final int separatorIndex) {
        Long expiration = null;
        String expirationAsString = name.substring(separatorIndex + 1);
        try {
            // calculate expiration, support arithmetic expressions.
            if (PATTERN.matcher(expirationAsString).find()) {
                expiration = (long) Double.parseDouble(SCRIPT_ENGINE.eval(expirationAsString).toString());
            }else{
                expiration = Long.parseLong(expirationAsString);
            }
        } catch (NumberFormatException ex) {
            log.error(String.format("Cannnot separate expiration time from cache: '%s'", name), ex);
        } catch (ScriptException e) {
            log.error(String.format("Cannnot separate expiration time from cache: '%s'", name), e);
        }

        return expiration;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setUsePrefix(boolean usePrefix) {
        super.setUsePrefix(usePrefix);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setCachePrefix(RedisCachePrefix cachePrefix) {
        super.setCachePrefix(cachePrefix);
    }

    public void setDefaultCacheName(String defaultCacheName) {
        this.defaultCacheName = defaultCacheName;
    }

    public long getDefaultExpiration() {
        return defaultExpiration;
    }

    @Override
    public void setDefaultExpiration(long defaultExpiration) {
        this.defaultExpiration = defaultExpiration;
    }
}
