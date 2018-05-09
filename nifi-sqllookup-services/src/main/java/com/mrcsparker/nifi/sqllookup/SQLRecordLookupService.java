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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"dbcp", "database", "lookup", "record", "sql", "cache"})
@CapabilityDescription(
    "Provides a lookup service based around DBCP."
)
public class SQLRecordLookupService extends AbstractSQLLookupService<Record> {

    static final Logger LOG = LoggerFactory.getLogger(SQLRecordLookupService.class);
    private final List<PropertyDescriptor> propertyDescriptors;

    public SQLRecordLookupService() {
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(CONNECTION_POOL);
        pds.add(SQL_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(CACHE_SIZE);
        propertyDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return KEYS;
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    Optional<Record> paramDatabaseLookup(String key) throws LookupFailureException {

        try (final Connection connection = dbcpService.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {

            preparedStatement.setString(1, key);
            preparedStatement.setQueryTimeout(queryTimeout);
            preparedStatement.execute();

            final ResultSet resultSet = preparedStatement.getResultSet();
            final RecordSchema schema = new SimpleRecordSchema(new ArrayList<>());
            final ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, schema);

            return Optional.of(resultSetRecordSet.next());

        } catch (final ProcessException | SQLException e) {
            getLogger().error("Error during lookup: {}", new Object[] { key }, e);
            throw new LookupFailureException(e);
        } catch (final NullPointerException | IOException e) {
            return Optional.empty();
        }
    }

    @Override
    Optional<Record> namedParamDatabaseLookup(String key) throws LookupFailureException {

        final DataSource dataSource = new BasicDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return dbcpService.getConnection();
            }
        };

        SQLNamedParameterJdbcTemplate jdbcTemplate = new SQLNamedParameterJdbcTemplate(dataSource);

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

        PreparedStatementCreator preparedStatementCreator = jdbcTemplate.getPreparedStatement(sqlQuery, mapSqlParameterSource);

        try (final Connection connection = dbcpService.getConnection();
             final PreparedStatement preparedStatement = preparedStatementCreator.createPreparedStatement(connection)) {

            preparedStatement.setQueryTimeout(queryTimeout);
            preparedStatement.execute();

            final ResultSet resultSet = preparedStatement.getResultSet();
            final RecordSchema schema = new SimpleRecordSchema(new ArrayList<>());
            final ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, schema);

            return Optional.of(resultSetRecordSet.next());

        } catch (final ProcessException | SQLException e) {
            getLogger().error("Error during lookup: {}", new Object[] { key }, e);
            throw new LookupFailureException(e);
        } catch (final NullPointerException | IOException e) {
            return Optional.empty();
        }
    }

    @Override
    protected Optional<Record> cacheLookup(String key) throws LookupFailureException {
        String cacheKey = key + ":" + sqlQuery;

        Record record = cache.getIfPresent(cacheKey);
        if (record != null) {
            return Optional.of(record);
        }

        Optional<Record> lookupRecord = databaseLookup(key);
        if (lookupRecord.isPresent()) {
            cache.put(cacheKey, lookupRecord.get());

            return lookupRecord;
        }

        return Optional.empty();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        this.sqlQuery = context.getProperty(SQL_QUERY).evaluateAttributeExpressions().getValue();
        this.queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        this.cacheSize = context.getProperty(CACHE_SIZE).asInteger();

        cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }
}
