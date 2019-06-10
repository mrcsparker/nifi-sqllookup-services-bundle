package com.mrcsparker.nifi.sqllookup;

import com.mrcsparker.nifi.sqllookup.cache.Cache2kAdapter;
import com.mrcsparker.nifi.sqllookup.cache.CacheAdapter;
import com.mrcsparker.nifi.sqllookup.cache.CaffeineAdapter;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    static final AllowableValue CACHING_LIBRARY_CAFFEINE = new AllowableValue("Caffeine", "Caffeine", "Use Caffeine");

    static final AllowableValue CACHING_LIBRARY_CACHE2k = new AllowableValue("Cache2k", "Cache2k", "Use Cache2k");

    static final PropertyDescriptor CACHING_LIBRARY = new PropertyDescriptor.Builder()
            .name("caching-library")
            .displayName("Caching library")
            .description("Library to use for caching.")
            .allowableValues(CACHING_LIBRARY_CAFFEINE, CACHING_LIBRARY_CACHE2k)
            .defaultValue(CACHING_LIBRARY_CAFFEINE.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache size")
            .description("Size of the lookup cache.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR  )
            .build();

    String sqlQuery;
    Integer queryTimeout;
    DBCPService dbcpService;

    CacheAdapter<T> cache;

    String cachingLibrary;
    Integer cacheSize;

    Boolean useJDBCTypes;

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

    long getCacheSize() {
        return cache.estimatedSize();
    }

    @OnDisabled
    public void onDisabled() {
        cache.cleanUp();
    }

    void setDefaultValues(final ConfigurationContext context) {
        this.dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        this.sqlQuery = context.getProperty(SQL_QUERY).evaluateAttributeExpressions().getValue();
        this.queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        this.cachingLibrary = context.getProperty(CACHING_LIBRARY).getValue();
        this.cacheSize = context.getProperty(CACHE_SIZE).asInteger();
    }
}
