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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"dbcp", "database", "lookup", "record", "sql", "cache"})
@CapabilityDescription(
    "Provides a lookup service based around DBCP."
)
public class SQLRecordLookupService extends AbstractSQLLookupService<Record> {

    static final Logger LOG = LoggerFactory.getLogger(SQLRecordLookupService.class);
    private final List<PropertyDescriptor> propertyDescriptors;

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

    public SQLRecordLookupService() {
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(CONNECTION_POOL);
        pds.add(SQL_QUERY);
        pds.add(QUERY_TIMEOUT);
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


    @Override
    Optional<Record> databaseLookup(Map<String, Object> coordinates) throws LookupFailureException {
        if (useJDBCTypes) {
            return jdbcDatabaseLookup(coordinates);
        }
        return resultRecordSetDatabaseLookup(coordinates);
    }

    Optional<Record> resultRecordSetDatabaseLookup(Map<String, Object> coordinates) throws LookupFailureException {
        final DataSource dataSource = new BasicDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return dbcpService.getConnection();
            }
        };

        SQLNamedParameterJdbcTemplate jdbcTemplate = new SQLNamedParameterJdbcTemplate(dataSource);
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();

        for (String column : coordinates.keySet()) {
            mapSqlParameterSource.addValue(column, coordinates.get(column));
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
            getLogger().error("Error during lookup: {}", new Object[] { coordinates.toString() }, e);
            throw new LookupFailureException(e);
        } catch (final NullPointerException | IOException e) {
            return Optional.empty();
        }
    }

    Optional<Record> jdbcDatabaseLookup(Map<String, Object> coordinates) throws LookupFailureException {
        final DataSource dataSource = new BasicDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return dbcpService.getConnection();
            }
        };

        SQLNamedParameterJdbcTemplate jdbcTemplate = new SQLNamedParameterJdbcTemplate(dataSource);
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();

        for (String column : coordinates.keySet()) {
            mapSqlParameterSource.addValue(column, coordinates.get(column));
        }

        PreparedStatementCreator preparedStatementCreator = jdbcTemplate.getPreparedStatement(sqlQuery, mapSqlParameterSource);

        try (final Connection connection = dbcpService.getConnection();
             final PreparedStatement preparedStatement = preparedStatementCreator.createPreparedStatement(connection)) {

            preparedStatement.setQueryTimeout(queryTimeout);
            preparedStatement.execute();

            final ResultSet resultSet = preparedStatement.getResultSet();
            final ResultSetMetaData metaData = resultSet.getMetaData();
            List<RecordField> fields = new ArrayList<>();
            Map<String, Object> results = new HashMap<>();

            if (resultSet.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String column = metaData.getColumnLabel(i);
                    int sqlType = metaData.getColumnType(i);
                    fields.add(new RecordField(column, JDBCType.getType(sqlType)));
                    Object value = JDBCType.getValue(resultSet, sqlType, i);
                    results.put(column, value);
                }
                return Optional.of(new MapRecord(new SimpleRecordSchema(fields), results));
            }
            return Optional.empty();


        } catch (final ProcessException | SQLException e) {
            getLogger().error("Error during lookup: {}", new Object[]{coordinates.toString()}, e);
            throw new LookupFailureException(e);
        } catch (final NullPointerException e) {
            return Optional.empty();
        }
    }

    @Override
    protected Optional<Record> cacheLookup(Map<String, Object> coordinates) throws LookupFailureException {
        String cacheKey = sqlQuery + ":" + coordinates.hashCode();

        Record record = cache.getIfPresent(cacheKey);
        if (record != null) {
            return Optional.of(record);
        }

        Optional<Record> lookupRecord = databaseLookup(coordinates);
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
        this.useJDBCTypes = context.getProperty(USE_JDBC_TYPES).asBoolean();

        cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }
}
