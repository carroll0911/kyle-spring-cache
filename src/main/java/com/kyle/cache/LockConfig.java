package com.kyle.cache;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 分布式锁配置
 *
 * @author: carroll
 * @date 2019/4/2
 * Copyright @https://github.com/carroll0911. 
 */
@Component
@ConfigurationProperties(prefix = "lock")
public class LockConfig {

    //重试次数
    private int retryTimes = 10;
    //重试间隔时间-毫秒
    private long sleepMillis = 100;

    private long defaultExpireMs = 2000;

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public long getSleepMillis() {
        return sleepMillis;
    }

    public void setSleepMillis(long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    public long getDefaultExpireMs() {
        return defaultExpireMs;
    }

    public void setDefaultExpireMs(long defaultExpireMs) {
        this.defaultExpireMs = defaultExpireMs;
    }
}
