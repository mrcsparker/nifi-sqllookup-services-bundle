package org.apache.nifi.sqllookup;

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singleton;

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

    private static final String KEY = "key";
    static final Set<String> KEYS = singleton(KEY);
    Integer cacheSize;

    @Override
    public Optional<T> lookup(Map<String, Object> coordinates) throws LookupFailureException {

        if (coordinates == null || coordinates.size() != 1) {
            return Optional.empty();
        }

        if (!coordinates.containsKey(KEY)) {
            throw new LookupFailureException("Required key " + KEY + " was not present in the lookup.");
        }

        String key = coordinates.get(KEY).toString();

        if (cacheSize > 0) {
            return cacheLookup(key);
        }

        return databaseLookup(key);
    }

    public Optional<T> databaseLookup(String key) throws LookupFailureException {
        if (isJson(key)) {
            return namedParamDatabaseLookup(key);
        }
        return paramDatabaseLookup(key);
    }

    abstract Optional<T> paramDatabaseLookup(String key) throws LookupFailureException;

    abstract Optional<T> namedParamDatabaseLookup(String key) throws LookupFailureException;

    abstract Optional<T> cacheLookup(String key) throws LookupFailureException;

    protected boolean isJson(String value) {
        if (value.isEmpty()) return false;
        return value.trim().toCharArray()[0] == '{';
    }

    @Override
    public Set<String> getRequiredKeys() {
        return KEYS;
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
