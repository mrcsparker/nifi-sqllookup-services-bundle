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
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
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

@Tags({"dbcp", "database", "lookup", "record", "sql", "cache"})
@CapabilityDescription(
        "Provides a lookup service based around DBCP."
)
public class SQLRecordLookupService extends AbstractSQLLookupService<Record> {

    static final Logger LOG = LoggerFactory.getLogger(SQLRecordLookupService.class);
    static final PropertyDescriptor USE_JDBC_TYPES =
            new PropertyDescriptor.Builder()
                    .name("use-jdbc-types")
                    .displayName("Use JDBC types")
                    .description("Use Built-in JDBC to Record type conversion.\n" +
                            "If this is not selected it will use the NIFI ResultRecordSet to convert into a Record.\n" +
                            "Use this if you are returning array types from a SQL query.")
                    .defaultValue("false")
                    .allowableValues("true", "false")
                    .required(true)
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .build();
    private final List<PropertyDescriptor> propertyDescriptors;

    public SQLRecordLookupService() {
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(CONNECTION_POOL);
        pds.add(SQL_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(CACHING_LIBRARY);
        pds.add(CACHE_SIZE);
        pds.add(USE_JDBC_TYPES);
        propertyDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    private PreparedStatementCreator setupPreparedStatementCreator(Map<String, Object> coordinates) {
        final DataSource dataSource = new BasicDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return dbcpService.getConnection();
            }
        };

        SQLNamedParameterJdbcTemplate jdbcTemplate = new SQLNamedParameterJdbcTemplate(dataSource);
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();

        for (Map.Entry<String, Object> column : coordinates.entrySet()) {
            mapSqlParameterSource.addValue(column.getKey(), coordinates.get(column.getKey()));
        }

        return jdbcTemplate.getPreparedStatement(sqlQuery, mapSqlParameterSource);
    }

    @Override
    Optional<Record> databaseLookup(Map<String, Object> coordinates) throws LookupFailureException {
        if (useJDBCTypes) {
            return jdbcDatabaseLookup(coordinates);
        }
        return resultRecordSetDatabaseLookup(coordinates);
    }

    private Optional<Record> resultRecordSetDatabaseLookup(Map<String, Object> coordinates) throws LookupFailureException {
        PreparedStatementCreator preparedStatementCreator = setupPreparedStatementCreator(coordinates);

        try (final Connection connection = dbcpService.getConnection();
             final PreparedStatement preparedStatement = preparedStatementCreator.createPreparedStatement(connection)) {

            preparedStatement.setQueryTimeout(queryTimeout);
            preparedStatement.execute();

            try (ResultSet resultSet = preparedStatement.getResultSet()) {
                final RecordSchema schema = new SimpleRecordSchema(new ArrayList<>());
                try (ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, schema)) {
                    return Optional.of(resultSetRecordSet.next());
                }
            }

        } catch (final ProcessException | SQLException e) {
            getLogger().error("Error during lookup: {}", new Object[]{coordinates.toString()}, e);
            throw new LookupFailureException(e);
        } catch (final NullPointerException | IOException e) {
            return Optional.empty();
        }
    }

    private Optional<Record> jdbcDatabaseLookup(Map<String, Object> coordinates) throws LookupFailureException {
        PreparedStatementCreator preparedStatementCreator = setupPreparedStatementCreator(coordinates);

        try (final Connection connection = dbcpService.getConnection();
             final PreparedStatement preparedStatement = preparedStatementCreator.createPreparedStatement(connection)) {

            preparedStatement.setQueryTimeout(queryTimeout);
            preparedStatement.execute();

            try (ResultSet resultSet = preparedStatement.getResultSet()) {
                final RecordSchema schema = new SimpleRecordSchema(new ArrayList<>());
                try (SQLResultSetRecordSet resultSetRecordSet = new SQLResultSetRecordSet(resultSet, schema)) {
                    return Optional.of(resultSetRecordSet.next());
                }
            }

        } catch (final ProcessException | SQLException e) {
            getLogger().error("Error during lookup: {}", new Object[]{coordinates.toString()}, e);
            throw new LookupFailureException(e);
        } catch (final NullPointerException | IOException e) {
            return Optional.empty();
        }
    }

    @Override
    protected Optional<Record> cacheLookup(Map<String, Object> coordinates) throws LookupFailureException {
        String cacheKey = sqlQuery + ":" + coordinates.hashCode();

        Record record = cache.get(cacheKey);
        if (record != null) {
            return Optional.of(record);
        }

        Optional<Record> lookupRecord = databaseLookup(coordinates);
        if (lookupRecord.isPresent()) {
            cache.set(cacheKey, lookupRecord.get());

            return lookupRecord;
        }

        return Optional.empty();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        setDefaultValues(context);
        this.useJDBCTypes = context.getProperty(USE_JDBC_TYPES).asBoolean();

        if (cachingLibrary.equals("Caffeine")) {
            cache = new CaffeineAdapter<>(cacheSize);
        } else {
            cache = new Cache2kAdapter<>(cacheSize, Record.class);
        }
    }
}
