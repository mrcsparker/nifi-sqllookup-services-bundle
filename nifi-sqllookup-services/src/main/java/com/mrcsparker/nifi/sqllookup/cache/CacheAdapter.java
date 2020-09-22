package com.mrcsparker.nifi.sqllookup.cache;

import java.util.concurrent.ConcurrentMap;

public interface CacheAdapter<T> {

    T get(String key);

    void set(String key, T value);

    void delete(String key);

    long estimatedSize();

    ConcurrentMap<String, T> asMap();

    void cleanUp();
}
