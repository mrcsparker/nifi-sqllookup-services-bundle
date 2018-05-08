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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SQLLookupService extends AbstractSQLLookupService<String> {

    static final Logger LOG = LoggerFactory.getLogger(SQLLookupService.class);

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
        if (isJson(key)) {
            return namedParamDatabaseLookup(key);
        }
        return paramDatabaseLookup(key);
    }

    Optional<String> paramDatabaseLookup(String key) throws LookupFailureException {
        try (final Connection connection = dbcpService.getConnection();
                final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {

            preparedStatement.setString(1, key);
            preparedStatement.setQueryTimeout(queryTimeout);
            preparedStatement.execute();

            final ResultSet resultSet = preparedStatement.getResultSet();
            final RecordSchema schema = new SimpleRecordSchema(new ArrayList<>());
            final ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, schema);

            Record record = resultSetRecordSet.next();
            return Optional.of(record.getAsString(lookupValue));

        } catch (final ProcessException | SQLException e) {
            getLogger().error("Error during lookup: {}", new Object[] { key }, e);
            throw new LookupFailureException(e);
        } catch (final NullPointerException | IOException e) {
            return Optional.empty();
        }
    }

    Optional<String> namedParamDatabaseLookup(String key) throws LookupFailureException {
        final DataSource dataSource = new BasicDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return dbcpService.getConnection();
            }
        };

        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);

        Map<String, Object> map;
        ObjectMapper mapper = new ObjectMapper();
        try {
            map = mapper.readValue(key, new TypeReference<Map<String, Object>>(){});
        } catch (IOException e) {
            getLogger().error("Error during lookup: {}", new Object[] { key }, e);
            return Optional.empty();
        }

        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        for (String col : map.keySet()) {
            mapSqlParameterSource.addValue(col, map.get(col));
        }

        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sqlQuery, mapSqlParameterSource);

        // LOG.info("{}", mapList);

        if (mapList.size() > 0) {
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

    @OnEnabled
    public void onEnabled(final ConfigurationContext context)  throws InitializationException {
        this.dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        this.sqlQuery = context.getProperty(SQL_QUERY).evaluateAttributeExpressions().getValue();
        this.queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        this.cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        this.lookupValue = context.getProperty(LOOKUP_VALUE_COLUMN).getValue();

        cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

}
