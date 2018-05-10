package com.mrcsparker.nifi.sqllookup;

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

abstract class AbstractSQLLookupService<T> extends AbstractControllerService implements LookupService<T> {

    static final PropertyDescriptor CONNECTION_POOL =
            new PropertyDescriptor.Builder()
                    .name("connection-pool")
                    .displayName("Connection Pool")
                    .description("Specifies the JDBC connection pool used to connect to the database.")
                    .identifiesControllerService(DBCPService.class)
                    .required(true)
                    .build();

    static final PropertyDescriptor SQL_QUERY =
            new PropertyDescriptor.Builder()
                    .name("sql-query")
                    .displayName("SQL Query")
                    .description("SQL Query")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(true)
                    .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("max-wait-time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .description("Size of the lookup cache.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR  )
            .build();

    String sqlQuery;
    Integer queryTimeout;
    DBCPService dbcpService;

    Cache<String, T> cache;

    Integer cacheSize;

    @Override
    public Optional<T> lookup(Map<String, Object> coordinates) throws LookupFailureException {

        if (coordinates == null || coordinates.size() == 0) {
            return Optional.empty();
        }

        if (cacheSize > 0) {
            return cacheLookup(coordinates);
        }

        return databaseLookup(coordinates);
    }

    abstract Optional<T> databaseLookup(Map<String, Object> coordinates) throws LookupFailureException;

    abstract Optional<T> cacheLookup(Map<String, Object> coordinates) throws LookupFailureException;


    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    public long getCacheSize() {
        return cache.estimatedSize();
    }

    @OnDisabled
    public void onDisabled() {
        cache.invalidateAll();
        cache.cleanUp();
    }
}
