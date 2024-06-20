package com.kyle.cache;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Redis Cache 配置
 *
 * @author carroll
 * @Date 2017-05-12 9:19
 **/
@Configuration
@EnableCaching
public class RedisCacheIniter extends CachingConfigurerSupport {

    @Autowired
    private CacheRedisConfig cacheRedisConfig;

    @Override
    @Bean
    public KeyGenerator keyGenerator() {
        return (target, method, params) -> {
            StringBuilder sb = new StringBuilder();
            sb.append(getTargetClass(target).getName());
            sb.append(method.getName());
            for (Object obj : params) {
                sb.append(obj == null ? "null" : obj.toString());
            }
            return sb.toString();
        };

    }

    private Class getTargetClass(Object target) {
        if (!AopUtils.isAopProxy(target)) {
            return target.getClass();
        } else {
            return AopUtils.getTargetClass(target);
        }
    }

    public RedisConnectionFactory redisConnectionFactory() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(cacheRedisConfig.getMaxIdle());
        jedisPoolConfig.setMaxTotal(cacheRedisConfig.getMaxTotal());
        jedisPoolConfig.setMaxWaitMillis(cacheRedisConfig.getMaxWaitMillis());

        JedisConnectionFactory jedisConnectionFactory;
        if (cacheRedisConfig.isClusterEnable()) {
            RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration(getNodes());
            jedisConnectionFactory = new JedisConnectionFactory(redisClusterConfiguration, jedisPoolConfig);
        } else {
            jedisConnectionFactory = new JedisConnectionFactory(jedisPoolConfig);
            JedisShardInfo shardInfo = new JedisShardInfo(cacheRedisConfig.getHost(), cacheRedisConfig.getPort(),
                    cacheRedisConfig.getTimeout());
            shardInfo.setConnectionTimeout(cacheRedisConfig.getTimeout());
            shardInfo.setPassword(cacheRedisConfig.getPassword());
            jedisConnectionFactory.setUsePool(true);
            jedisConnectionFactory.setShardInfo(shardInfo);
            jedisConnectionFactory.setHostName(cacheRedisConfig.getHost());
            jedisConnectionFactory.setPort(cacheRedisConfig.getPort());
        }
        jedisConnectionFactory.setTimeout(cacheRedisConfig.getTimeout());
        jedisConnectionFactory.setDatabase(cacheRedisConfig.getDatabase());
        jedisConnectionFactory.setPassword(cacheRedisConfig.getPassword());
        jedisConnectionFactory.afterPropertiesSet();
        return jedisConnectionFactory;
    }

    private List<String> getNodes() {
        List<String> nodes = new ArrayList<>();
        if (!StringUtils.isEmpty(cacheRedisConfig.getHost())) {
            nodes.add(cacheRedisConfig.getHost() + ":" + (StringUtils.isEmpty(cacheRedisConfig.getPort()) ? "27017" : cacheRedisConfig.getPort()));
        }
        if (!StringUtils.isEmpty(cacheRedisConfig.getClusterNodes())) {
            nodes.addAll(Arrays.asList(cacheRedisConfig.getClusterNodes().split(",")));
        }
        Assert.isTrue(!nodes.isEmpty(), "host or clusterNodes must not be null!");
        return nodes;
    }

    @Bean
    @Override
    public CacheManager cacheManager() {
        ExtendRedisCacheManager cacheManager = new ExtendRedisCacheManager(redisTemplate());
        cacheManager.setUsePrefix(cacheRedisConfig.isUsePrefix());
        if (cacheRedisConfig.isUsePrefix()) {
            cacheManager.setCachePrefix(new ExtendRedisCachePrefix(cacheRedisConfig.getCacheName()));
        }
        cacheManager.setDefaultExpiration(cacheRedisConfig.getDefaultExpiration());
        cacheManager.setDefaultCacheName(cacheRedisConfig.getCacheName());
        return cacheManager;
    }

    @Bean(name = "cacheRedisTemplate")
    public RedisTemplate<String, String> redisTemplate() {
        StringRedisTemplate template = new StringRedisTemplate(redisConnectionFactory());
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer(Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 解决jackson2无法反序列化LocalDateTime的问题
        om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        om.registerModule(new JavaTimeModule());
        jackson2JsonRedisSerializer.setObjectMapper(om);
        template.setValueSerializer(jackson2JsonRedisSerializer);
        template.afterPropertiesSet();
        return template;
    }
}
