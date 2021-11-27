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

package com.mrcsparker.nifi.sqllookup;

import com.mrcsparker.nifi.sqllookup.cache.Cache2kAdapter;
import com.mrcsparker.nifi.sqllookup.cache.CaffeineAdapter;
import com.mrcsparker.nifi.sqllookup.cache.GuavaAdapter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class SQLLookupService extends AbstractSQLLookupService<String> {

    public static final PropertyDescriptor LOOKUP_VALUE_COLUMN =
                    new PropertyDescriptor.Builder()
                                    .name("lookup-value-column")
                                    .displayName("Lookup Value Column")
                                    .description("Lookup value column.")
                                    .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                    .build();
    static final Logger LOG = LoggerFactory.getLogger(SQLLookupService.class);
    private final List<PropertyDescriptor> propertyDescriptors;
    private String lookupValue;

    public SQLLookupService() {
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(CONNECTION_POOL);
        pds.add(SQL_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(CACHING_LIBRARY);
        pds.add(CACHE_SIZE);
        pds.add(LOOKUP_VALUE_COLUMN);
        propertyDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    Optional<String> databaseLookup(Map<String, Object> coordinates) throws LookupFailureException {
        final DataSource dataSource = new BasicDataSource() {

            @Override
            public Connection getConnection() throws SQLException {
                return dbcpService.getConnection();
            }
        };

        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();

        for (Map.Entry<String, Object> column : coordinates.entrySet()) {
            mapSqlParameterSource.addValue(column.getKey(), coordinates.get(column.getKey()));
        }

        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sqlQuery, mapSqlParameterSource);

        if (!mapList.isEmpty()) {
            Object o = mapList.get(0).get(lookupValue);
            if (o == null) {
                return Optional.empty();
            } else {
                return Optional.of(o.toString());
            }
        }

        return Optional.empty();
    }

    @Override
    protected Optional<String> cacheLookup(Map<String, Object> coordinates) throws LookupFailureException {
        String cacheKey = sqlQuery + ":" + coordinates.hashCode();

        String result = cache.get(cacheKey);
        if (result != null) {
            return Optional.of(result);
        }

        Optional<String> lookupRecord = databaseLookup(coordinates);
        if (lookupRecord.isPresent()) {
            result = lookupRecord.get();

            cache.set(cacheKey, result);

            return lookupRecord;
        }

        return Optional.empty();
    }

    @Override
    public Class<?> getValueType() {
        return String.class;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        setDefaultValues(context);
        lookupValue = context.getProperty(LOOKUP_VALUE_COLUMN).getValue();

        switch (cachingLibrary) {
            case "Caffeine":
                cache = new CaffeineAdapter<>(cacheSize);
                break;
            case "Cache2k":
                cache = new Cache2kAdapter<>(cacheSize, String.class);
                break;
            default:
                cache = new GuavaAdapter<>(cacheSize);
                break;
        }
    }

}
