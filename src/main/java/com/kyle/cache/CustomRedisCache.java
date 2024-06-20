package com.kyle.cache;

import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.cache.RedisCacheElement;
import org.springframework.data.redis.cache.RedisCacheKey;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.*;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author: carroll
 * @date 2019/4/29
 * Copyright @https://github.com/carroll0911. 
 */
public class CustomRedisCache extends AbstractValueAdaptingCache {

    private final RedisOperations redisOperations;
    private final CustomRedisCache.RedisCacheMetadata cacheMetadata;
    private final CustomRedisCache.CacheValueAccessor cacheValueAccessor;

    public CustomRedisCache(String name, byte[] prefix, RedisOperations<? extends Object, ? extends Object> redisOperations, long expiration) {
        this(name, prefix, redisOperations, expiration, false);
    }

    public CustomRedisCache(String name, byte[] prefix, RedisOperations<? extends Object, ? extends Object> redisOperations, long expiration, boolean allowNullValues) {
        super(allowNullValues);
        Assert.hasText(name, "CacheName must not be null or empty!");
        RedisSerializer<?> serializer = redisOperations.getValueSerializer() != null ? redisOperations.getValueSerializer() : new JdkSerializationRedisSerializer();
        this.cacheMetadata = new CustomRedisCache.RedisCacheMetadata(name, prefix);
        this.cacheMetadata.setDefaultExpiration(expiration);
        this.redisOperations = redisOperations;
        this.cacheValueAccessor = new CustomRedisCache.CacheValueAccessor((RedisSerializer) serializer);
        if (allowNullValues && (redisOperations.getValueSerializer() instanceof StringRedisSerializer || redisOperations.getValueSerializer() instanceof GenericToStringSerializer || redisOperations.getValueSerializer() instanceof JacksonJsonRedisSerializer || redisOperations.getValueSerializer() instanceof Jackson2JsonRedisSerializer)) {
            throw new IllegalArgumentException(String.format("Redis does not allow keys with null value ¯\\_(ツ)_/¯. The chosen %s does not support generic type handling and therefore cannot be used with allowNullValues enabled. Please use a different RedisSerializer or disable null value support.", ClassUtils.getShortName(redisOperations.getValueSerializer().getClass())));
        }
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        ValueWrapper wrapper = this.get(key);
        return wrapper == null ? null : (T) wrapper.get();
    }

    @Override
    public ValueWrapper get(Object key) {
        return this.get(this.getRedisCacheKey(key));
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        RedisCacheElement cacheElement = (new RedisCacheElement(this.getRedisCacheKey(key), new CustomRedisCache.StoreTranslatingCallable(valueLoader))).expireAfter(this.cacheMetadata.getDefaultExpiration());
        CustomRedisCache.BinaryRedisCacheElement rce = new CustomRedisCache.BinaryRedisCacheElement(cacheElement, this.cacheValueAccessor);
        ValueWrapper val = this.get(key);
        if (val != null) {
            return (T) val.get();
        } else {
            CustomRedisCache.RedisWriteThroughCallback callback = new CustomRedisCache.RedisWriteThroughCallback(rce, this.cacheMetadata);

            try {
                byte[] result = (byte[]) ((byte[]) this.redisOperations.execute(callback));
                return result == null ? null : (T) this.fromStoreValue(this.cacheValueAccessor.deserializeIfNecessary(result));
            } catch (RuntimeException var8) {
                throw CustomRedisCache.CacheValueRetrievalExceptionFactory.INSTANCE.create(key, valueLoader, var8);
            }
        }
    }

    public RedisCacheElement get(final RedisCacheKey cacheKey) {
        Assert.notNull(cacheKey, "CacheKey must not be null!");
        Boolean exists = (Boolean) this.redisOperations.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.exists(cacheKey.getKeyBytes());
            }
        });
        return !exists ? null : new RedisCacheElement(cacheKey, this.fromStoreValue(this.lookup(cacheKey)));
    }

    @Override
    public void put(Object key, Object value) {
        this.put((new RedisCacheElement(this.getRedisCacheKey(key), this.toStoreValue(value))).expireAfter(this.cacheMetadata.getDefaultExpiration()));
    }

    @Override
    protected Object fromStoreValue(Object storeValue) {
        return this.isAllowNullValues() && storeValue instanceof NullValue ? null : super.fromStoreValue(storeValue);
    }

    public void put(RedisCacheElement element) {
        Assert.notNull(element, "Element must not be null!");
        this.redisOperations.execute(new CustomRedisCache.RedisCachePutCallback(new CustomRedisCache.BinaryRedisCacheElement(element, this.cacheValueAccessor), this.cacheMetadata));
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        return this.putIfAbsent((new RedisCacheElement(this.getRedisCacheKey(key), this.toStoreValue(value))).expireAfter(this.cacheMetadata.getDefaultExpiration()));
    }

    public ValueWrapper putIfAbsent(RedisCacheElement element) {
        Assert.notNull(element, "Element must not be null!");
        new CustomRedisCache.RedisCachePutIfAbsentCallback(new CustomRedisCache.BinaryRedisCacheElement(element, this.cacheValueAccessor), this.cacheMetadata);
        return this.toWrapper(this.cacheValueAccessor.deserializeIfNecessary((byte[]) ((byte[]) this.redisOperations.execute(new CustomRedisCache.RedisCachePutIfAbsentCallback(new CustomRedisCache.BinaryRedisCacheElement(element, this.cacheValueAccessor), this.cacheMetadata)))));
    }

    @Override
    public void evict(Object key) {
        this.evict(new RedisCacheElement(this.getRedisCacheKey(key), (Object) null));
    }

    public void evict(RedisCacheElement element) {
        Assert.notNull(element, "Element must not be null!");
        this.redisOperations.execute(new CustomRedisCache.RedisCacheEvictCallback(new CustomRedisCache.BinaryRedisCacheElement(element, this.cacheValueAccessor), this.cacheMetadata));
    }

    @Override
    public void clear() {
        this.redisOperations.execute((RedisCallback) (this.cacheMetadata.usesKeyPrefix() ? new CustomRedisCache.RedisCacheCleanByPrefixCallback(this.cacheMetadata) : new CustomRedisCache.RedisCacheCleanByKeysCallback(this.cacheMetadata)));
    }

    @Override
    public String getName() {
        return this.cacheMetadata.getCacheName();
    }

    @Override
    public Object getNativeCache() {
        return this.redisOperations;
    }

    private ValueWrapper toWrapper(Object value) {
        return value != null ? new SimpleValueWrapper(value) : null;
    }

    @Override
    protected Object lookup(Object key) {
        RedisCacheKey cacheKey = key instanceof RedisCacheKey ? (RedisCacheKey) key : this.getRedisCacheKey(key);
        byte[] bytes = (byte[]) ((byte[]) this.redisOperations.execute(new CustomRedisCache.AbstractRedisCacheCallback<byte[]>(new CustomRedisCache.BinaryRedisCacheElement(new RedisCacheElement(cacheKey, (Object) null), this.cacheValueAccessor), this.cacheMetadata) {
            @Override
            public byte[] doInRedis(CustomRedisCache.BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {
                return connection.get(element.getKeyBytes());
            }
        }));
        return bytes == null ? null : this.cacheValueAccessor.deserializeIfNecessary(bytes);
    }

    private RedisCacheKey getRedisCacheKey(Object key) {
        return (new RedisCacheKey(key)).usePrefix(this.cacheMetadata.getKeyPrefix()).withKeySerializer(this.redisOperations.getKeySerializer());
    }

    private static boolean isClusterConnection(RedisConnection connection) {
        while (connection instanceof DecoratedRedisConnection) {
            connection = ((DecoratedRedisConnection) connection).getDelegate();
        }

        return connection instanceof RedisClusterConnection;
    }

    private static enum CacheValueRetrievalExceptionFactory {
        INSTANCE;

        private static boolean isSpring43 = ClassUtils.isPresent("org.springframework.cache.Cache$ValueRetrievalException", ClassUtils.getDefaultClassLoader());

        private CacheValueRetrievalExceptionFactory() {
        }

        public RuntimeException create(Object key, Callable<?> valueLoader, Throwable cause) {
            if (isSpring43) {
                try {
                    Class<?> execption = ClassUtils.forName("org.springframework.cache.Cache$ValueRetrievalException", this.getClass().getClassLoader());
                    Constructor<?> c = ClassUtils.getConstructorIfAvailable(execption, new Class[]{Object.class, Callable.class, Throwable.class});
                    return (RuntimeException) c.newInstance(key, valueLoader, cause);
                } catch (Exception var6) {
                }
            }

            return new RedisSystemException(String.format("Value for key '%s' could not be loaded using '%s'.", key, valueLoader), cause);
        }
    }

    static class RedisWriteThroughCallback extends CustomRedisCache.AbstractRedisCacheCallback<byte[]> {
        public RedisWriteThroughCallback(CustomRedisCache.BinaryRedisCacheElement element, CustomRedisCache.RedisCacheMetadata metadata) {
            super(element, metadata);
        }

        @Override
        public byte[] doInRedis(CustomRedisCache.BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {
            byte[] var4;
            try {
                this.lock(connection);

                try {
                    byte[] value = connection.get(element.getKeyBytes());
                    if (value == null) {
                        if (!CustomRedisCache.isClusterConnection(connection)) {
                            connection.watch(new byte[][]{element.getKeyBytes()});
                            connection.multi();
                        }

                        value = element.get();
                        if (value.length == 0) {
                            connection.del(new byte[][]{element.getKeyBytes()});
                        } else {
                            connection.set(element.getKeyBytes(), value);
                            this.processKeyExpiration(element, connection);
                            this.maintainKnownKeys(element, connection);
                        }

                        if (!CustomRedisCache.isClusterConnection(connection)) {
                            connection.exec();
                        }

                        var4 = value;
                        return var4;
                    }

                    var4 = value;
                } catch (RuntimeException var8) {
                    if (!CustomRedisCache.isClusterConnection(connection)) {
                        connection.discard();
                    }

                    throw var8;
                }
            } finally {
                this.unlock(connection);
            }

            return var4;
        }
    }

    static class RedisCachePutIfAbsentCallback extends CustomRedisCache.AbstractRedisCacheCallback<byte[]> {
        public RedisCachePutIfAbsentCallback(CustomRedisCache.BinaryRedisCacheElement element, CustomRedisCache.RedisCacheMetadata metadata) {
            super(element, metadata);
        }

        @Override
        public byte[] doInRedis(CustomRedisCache.BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {
            this.waitForLock(connection);
            byte[] keyBytes = element.getKeyBytes();
            byte[] value = element.get();
            if (!connection.setNX(keyBytes, value)) {
                return connection.get(keyBytes);
            } else {
                this.maintainKnownKeys(element, connection);
                this.processKeyExpiration(element, connection);
                return null;
            }
        }
    }

    static class RedisCachePutCallback extends CustomRedisCache.AbstractRedisCacheCallback<Void> {
        public RedisCachePutCallback(CustomRedisCache.BinaryRedisCacheElement element, CustomRedisCache.RedisCacheMetadata metadata) {
            super(element, metadata);
        }

        @Override
        public Void doInRedis(CustomRedisCache.BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {
            if (!CustomRedisCache.isClusterConnection(connection)) {
                connection.multi();
            }

            if (element.get().length == 0) {
                connection.del(new byte[][]{element.getKeyBytes()});
            } else {
                connection.set(element.getKeyBytes(), element.get());
                this.processKeyExpiration(element, connection);
                this.maintainKnownKeys(element, connection);
            }

            if (!CustomRedisCache.isClusterConnection(connection)) {
                connection.exec();
            }

            return null;
        }
    }

    static class RedisCacheEvictCallback extends CustomRedisCache.AbstractRedisCacheCallback<Void> {
        public RedisCacheEvictCallback(CustomRedisCache.BinaryRedisCacheElement element, CustomRedisCache.RedisCacheMetadata metadata) {
            super(element, metadata);
        }

        @Override
        public Void doInRedis(CustomRedisCache.BinaryRedisCacheElement element, RedisConnection connection) throws DataAccessException {
            connection.del(new byte[][]{element.getKeyBytes()});
            this.cleanKnownKeys(element, connection);
            return null;
        }
    }

    static class RedisCacheCleanByPrefixCallback extends CustomRedisCache.LockingRedisCacheCallback<Void> {
        private static final byte[] REMOVE_KEYS_BY_PATTERN_LUA = (new StringRedisSerializer()).serialize("local keys = redis.call('KEYS', ARGV[1]); local keysCount = table.getn(keys); if(keysCount > 0) then for _, key in ipairs(keys) do redis.call('del', key); end; end; return keysCount;");
        private static final byte[] WILD_CARD = (new StringRedisSerializer()).serialize("*");
        private final CustomRedisCache.RedisCacheMetadata metadata;

        public RedisCacheCleanByPrefixCallback(CustomRedisCache.RedisCacheMetadata metadata) {
            super(metadata);
            this.metadata = metadata;
        }

        @Override
        public Void doInLock(RedisConnection connection) throws DataAccessException {
            byte[] prefixToUse = Arrays.copyOf(this.metadata.getKeyPrefix(), this.metadata.getKeyPrefix().length + WILD_CARD.length);
            System.arraycopy(WILD_CARD, 0, prefixToUse, this.metadata.getKeyPrefix().length, WILD_CARD.length);
            if (CustomRedisCache.isClusterConnection(connection)) {
                Set<byte[]> keys = connection.keys(prefixToUse);
                if (!keys.isEmpty()) {
                    connection.del((byte[][]) keys.toArray(new byte[keys.size()][]));
                }
            } else {
                connection.eval(REMOVE_KEYS_BY_PATTERN_LUA, ReturnType.INTEGER, 0, new byte[][]{prefixToUse});
            }

            return null;
        }
    }

    static class RedisCacheCleanByKeysCallback extends CustomRedisCache.LockingRedisCacheCallback<Void> {
        private static final int PAGE_SIZE = 128;
        private final CustomRedisCache.RedisCacheMetadata metadata;

        RedisCacheCleanByKeysCallback(CustomRedisCache.RedisCacheMetadata metadata) {
            super(metadata);
            this.metadata = metadata;
        }

        @Override
        public Void doInLock(RedisConnection connection) {
            int offset = 0;
            boolean finished = false;

            do {
                Set<byte[]> keys = connection.zRange(this.metadata.getSetOfKnownKeysKey(), (long) (offset * 128), (long) ((offset + 1) * 128 - 1));
                finished = keys.size() < 128;
                ++offset;
                if (!keys.isEmpty()) {
                    connection.del((byte[][]) keys.toArray(new byte[keys.size()][]));
                }
            } while (!finished);

            connection.del(new byte[][]{this.metadata.getSetOfKnownKeysKey()});
            return null;
        }
    }

    abstract static class LockingRedisCacheCallback<T> implements RedisCallback<T> {
        private final CustomRedisCache.RedisCacheMetadata metadata;

        public LockingRedisCacheCallback(CustomRedisCache.RedisCacheMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public T doInRedis(RedisConnection connection) throws DataAccessException {
            if (connection.exists(this.metadata.getCacheLockKey())) {
                return null;
            } else {
                Object var2;
                try {
                    connection.set(this.metadata.getCacheLockKey(), this.metadata.getCacheLockKey());
                    var2 = this.doInLock(connection);
                } finally {
                    connection.del(new byte[][]{this.metadata.getCacheLockKey()});
                }

                return (T) var2;
            }
        }

        public abstract T doInLock(RedisConnection var1);
    }

    abstract static class AbstractRedisCacheCallback<T> implements RedisCallback<T> {
        private long WAIT_FOR_LOCK_TIMEOUT = 300L;
        private final CustomRedisCache.BinaryRedisCacheElement element;
        private final CustomRedisCache.RedisCacheMetadata cacheMetadata;

        public AbstractRedisCacheCallback(CustomRedisCache.BinaryRedisCacheElement element, CustomRedisCache.RedisCacheMetadata metadata) {
            this.element = element;
            this.cacheMetadata = metadata;
        }

        @Override
        public T doInRedis(RedisConnection connection) throws DataAccessException {
            this.waitForLock(connection);
            return this.doInRedis(this.element, connection);
        }

        public abstract T doInRedis(CustomRedisCache.BinaryRedisCacheElement var1, RedisConnection var2) throws DataAccessException;

        protected void processKeyExpiration(RedisCacheElement element, RedisConnection connection) {
            if (!element.isEternal()) {
                connection.expire(element.getKeyBytes(), element.getTimeToLive());
            }

        }

        protected void maintainKnownKeys(RedisCacheElement element, RedisConnection connection) {
            if (!element.hasKeyPrefix()) {
                connection.zAdd(this.cacheMetadata.getSetOfKnownKeysKey(), 0.0D, element.getKeyBytes());
                if (!element.isEternal()) {
                    connection.expire(this.cacheMetadata.getSetOfKnownKeysKey(), element.getTimeToLive());
                }
            }

        }

        protected void cleanKnownKeys(RedisCacheElement element, RedisConnection connection) {
            if (!element.hasKeyPrefix()) {
                connection.zRem(this.cacheMetadata.getSetOfKnownKeysKey(), new byte[][]{element.getKeyBytes()});
            }

        }

        protected boolean waitForLock(RedisConnection connection) {
            boolean foundLock = false;

            boolean retry;
            do {
                retry = false;
                if (connection.exists(this.cacheMetadata.getCacheLockKey())) {
                    foundLock = true;

                    try {
                        Thread.sleep(this.WAIT_FOR_LOCK_TIMEOUT);
                    } catch (InterruptedException var5) {
                        Thread.currentThread().interrupt();
                    }

                    retry = true;
                }
            } while (retry);

            return foundLock;
        }

        protected void lock(RedisConnection connection) {
            this.waitForLock(connection);
            connection.set(this.cacheMetadata.getCacheLockKey(), "locked".getBytes(), Expiration.seconds(2), RedisStringCommands.SetOption.UPSERT);
        }

        protected void unlock(RedisConnection connection) {
            connection.del(new byte[][]{this.cacheMetadata.getCacheLockKey()});
        }
    }

    static class BinaryRedisCacheElement extends RedisCacheElement {
        private byte[] keyBytes;
        private byte[] valueBytes;
        private RedisCacheElement element;
        private boolean lazyLoad;
        private CustomRedisCache.CacheValueAccessor accessor;

        public BinaryRedisCacheElement(RedisCacheElement element, CustomRedisCache.CacheValueAccessor accessor) {
            super(element.getKey(), element.get());
            this.element = element;
            this.keyBytes = element.getKeyBytes();
            this.accessor = accessor;
            this.lazyLoad = element.get() instanceof Callable;
            this.valueBytes = this.lazyLoad ? null : accessor.convertToBytesIfNecessary(element.get());
        }

        @Override
        public byte[] getKeyBytes() {
            return this.keyBytes;
        }

        @Override
        public long getTimeToLive() {
            return this.element.getTimeToLive();
        }

        @Override
        public boolean hasKeyPrefix() {
            return this.element.hasKeyPrefix();
        }

        @Override
        public boolean isEternal() {
            return this.element.isEternal();
        }

        @Override
        public RedisCacheElement expireAfter(long seconds) {
            return this.element.expireAfter(seconds);
        }

        @Override
        public byte[] get() {
            if (this.lazyLoad && this.valueBytes == null) {
                try {
                    this.valueBytes = this.accessor.convertToBytesIfNecessary(((Callable) this.element.get()).call());
                } catch (Exception var2) {
                    throw var2 instanceof RuntimeException ? (RuntimeException) var2 : new RuntimeException(var2.getMessage(), var2);
                }
            }

            return this.valueBytes;
        }
    }

    static class CacheValueAccessor {
        private final RedisSerializer valueSerializer;

        CacheValueAccessor(RedisSerializer valueRedisSerializer) {
            this.valueSerializer = valueRedisSerializer;
        }

        byte[] convertToBytesIfNecessary(Object value) {
            if (value == null) {
                return new byte[0];
            } else {
                return this.valueSerializer == null && value instanceof byte[] ? (byte[]) ((byte[]) value) : this.valueSerializer.serialize(value);
            }
        }

        Object deserializeIfNecessary(byte[] value) {
            return this.valueSerializer != null ? this.valueSerializer.deserialize(value) : value;
        }
    }

    static class RedisCacheMetadata {
        private final String cacheName;
        private final byte[] keyPrefix;
        private final byte[] setOfKnownKeys;
        private final byte[] cacheLockName;
        private long defaultExpiration = 0L;

        public RedisCacheMetadata(String cacheName, byte[] keyPrefix) {
            Assert.hasText(cacheName, "CacheName must not be null or empty!");
            this.cacheName = cacheName;
            this.keyPrefix = keyPrefix;
            StringRedisSerializer stringSerializer = new StringRedisSerializer();
            this.setOfKnownKeys = this.usesKeyPrefix() ? new byte[0] : stringSerializer.serialize(cacheName + "~keys");
            this.cacheLockName = stringSerializer.serialize(cacheName + "~lock");
        }

        public boolean usesKeyPrefix() {
            return this.keyPrefix != null && this.keyPrefix.length > 0;
        }

        public byte[] getKeyPrefix() {
            return this.keyPrefix;
        }

        public byte[] getSetOfKnownKeysKey() {
            return this.setOfKnownKeys;
        }

        public byte[] getCacheLockKey() {
            return this.cacheLockName;
        }

        public String getCacheName() {
            return this.cacheName;
        }

        public void setDefaultExpiration(long seconds) {
            this.defaultExpiration = seconds;
        }

        public long getDefaultExpiration() {
            return this.defaultExpiration;
        }
    }

    private class StoreTranslatingCallable implements Callable<Object> {
        private Callable<?> valueLoader;

        public StoreTranslatingCallable(Callable<?> valueLoader) {
            this.valueLoader = valueLoader;
        }

        @Override
        public Object call() throws Exception {
            return CustomRedisCache.this.toStoreValue(this.valueLoader.call());
        }
    }
}
