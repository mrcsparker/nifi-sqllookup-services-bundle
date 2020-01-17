package com.mrcsparker.nifi.sqllookup.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.ConcurrentMap;

public class CaffeineAdapter<T> extends CacheAdapter<T> {

    private final Cache<String, T> cache;

    public CaffeineAdapter(Integer cacheSize) {
        cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    @Override
    public T get(String key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void set(String key, T value) {
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.invalidate(key);
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    public ConcurrentMap<String, T> asMap() {
        return cache.asMap();
    }

    @Override
    public void cleanUp() {
        cache.invalidateAll();
        cache.cleanUp();
    }
}
