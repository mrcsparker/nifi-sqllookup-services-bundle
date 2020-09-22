package com.mrcsparker.nifi.sqllookup.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.ConcurrentMap;

public class GuavaAdapter<T> implements CacheAdapter<T> {

    Cache<String, T> cache;

    public GuavaAdapter(Integer cacheSize) {
        cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
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
        return cache.size();
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
