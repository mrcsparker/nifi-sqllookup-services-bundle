package com.mrcsparker.nifi.sqllookup.cache;

import java.util.concurrent.ConcurrentMap;

public abstract class CacheAdapter<T> {
    public abstract T get(String key);

    public abstract void set(String key, T value);

    public abstract void delete(String key);

    public abstract long estimatedSize();

    public abstract ConcurrentMap<String, T> asMap();

    public abstract void cleanUp();
}
