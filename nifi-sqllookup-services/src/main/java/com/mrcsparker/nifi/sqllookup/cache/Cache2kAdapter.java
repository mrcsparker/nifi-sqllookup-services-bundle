package com.mrcsparker.nifi.sqllookup.cache;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import java.util.concurrent.ConcurrentMap;

public class Cache2kAdapter<T> extends CacheAdapter<T> {

    private final Cache<String, T> cache;

    public Cache2kAdapter(Integer cacheSize, Class<T> valueType) {
        cache = Cache2kBuilder.of(String.class, valueType)
                .entryCapacity(cacheSize).build();
    }

    @Override
    public T get(String key) {
        return cache.get(key);
    }

    @Override
    public void set(String key, T value) {
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }

    @Override
    public long estimatedSize() {
        return cache.asMap().values().size();
    }

    @Override
    public ConcurrentMap<String, T> asMap() {
        return cache.asMap();
    }

    @Override
    public void cleanUp() {
        cache.clear();
    }
}
