package com.carroll.cache;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author carroll
 * @Date 2017-07-25 18:06
 **/
@Component
@ConfigurationProperties(prefix = "cache.redis")
public class CacheRedisConfig {
    private boolean enable;
    private String host;
    private int port;
    private String password;
    private int database;
    private int maxWaitMillis = 10000;
    private int maxTotal;
    private int maxIdle;
    private int timeout;
    private String cacheName;
    private long defaultExpiration = 0;
    private String clusterNodes;
    private boolean clusterEnable;
    private boolean usePrefix = true;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public int getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public void setMaxWaitMillis(int maxWaitMillis) {
        this.maxWaitMillis = maxWaitMillis;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public long getDefaultExpiration() {
        return defaultExpiration;
    }

    public void setDefaultExpiration(long defaultExpiration) {
        this.defaultExpiration = defaultExpiration;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public boolean isClusterEnable() {
        return clusterEnable;
    }

    public void setClusterEnable(boolean clusterEnable) {
        this.clusterEnable = clusterEnable;
    }

    public boolean isUsePrefix() {
        return usePrefix;
    }

    public void setUsePrefix(boolean usePrefix) {
        this.usePrefix = usePrefix;
    }
}
