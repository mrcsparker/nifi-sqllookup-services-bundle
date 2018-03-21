/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.sqllookup;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.Record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SQLLookupService extends AbstractSQLLookupService<String> {

    public static final PropertyDescriptor LOOKUP_VALUE_COLUMN =
            new PropertyDescriptor.Builder()
                    .name("lookup-value-column")
                    .displayName("Lookup Value Column")
                    .description("Lookup value column.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(true)
                    .build();

    private String lookupValue;

    private final List<PropertyDescriptor> propertyDescriptors;

    public SQLLookupService() {
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(CONNECTION_POOL);
        pds.add(SQL_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(CACHE_SIZE);
        pds.add(LOOKUP_VALUE_COLUMN);
        propertyDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    Optional<String> databaseLookup(String key) throws LookupFailureException {

        try (final Connection connection = dbcpService.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {

            preparedStatement.setString(1, key);
            preparedStatement.setQueryTimeout(queryTimeout);
            preparedStatement.execute();

            ResultSet resultSet = preparedStatement.getResultSet();

            if (resultSet.next()) {
                final SQLRecordSet SQLRecordSet = new SQLRecordSet(resultSet);
                Optional<Record> lookupRecord = SQLRecordSet.getMapRecord();
                if (lookupRecord.isPresent()) {
                    return Optional.of(lookupRecord.get().getAsString(lookupValue));
                }
            }

        } catch (final ProcessException | SQLException e) {
            getLogger().error("Error during lookup: {}", new Object[] { key }, e);
            throw new LookupFailureException(e);
        }

        return Optional.empty();
    }

    @Override
    protected Optional<String> cacheLookup(String key) throws LookupFailureException {
        String cacheKey = key + ":" + sqlQuery;

        String result = cache.getIfPresent(cacheKey);
        if (result != null) {
            return Optional.of(result);
        }

        Optional<String> lookupRecord = databaseLookup(key);
        if (lookupRecord.isPresent()) {
            result = lookupRecord.get();

            cache.put(cacheKey, result);

            return lookupRecord;
        }

        return Optional.empty();
    }

    @Override
    public Class<?> getValueType() {
        return String.class;
    }

    public long getCacheSize() {
        return cache.estimatedSize();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        this.dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        this.sqlQuery = context.getProperty(SQL_QUERY).getValue();
        this.queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        this.cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        this.lookupValue = context.getProperty(LOOKUP_VALUE_COLUMN).getValue();

        cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    @OnDisabled
    public void onDisabled() {
        cache.invalidateAll();
        cache.cleanUp();
    }

}
