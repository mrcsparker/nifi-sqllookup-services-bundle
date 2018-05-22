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

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class TestSQLRecordLookupService extends AbstractSQLLookupServiceTest {

    static final Logger LOG = LoggerFactory.getLogger(TestSQLRecordLookupService.class);

    private SQLRecordLookupService sqlRecordLookupService;

    @Before
    public void before() throws Exception {
        TestProcessor testProcessor = new TestProcessor();
        runner = TestRunners.newTestRunner(testProcessor);

        // setup mock DBCP Service
        DBCPService dbcpService = new DBCPServiceSimpleImpl();
        Map<String, String> dbcpProperties = new HashMap<>();

        runner.addControllerService("dbcpService", dbcpService, dbcpProperties);
        runner.assertValid(dbcpService);

        // setup SQLRecordLookupService
        sqlRecordLookupService = new SQLRecordLookupService();
        runner.addControllerService("SQLRecordLookupService", sqlRecordLookupService);
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.CONNECTION_POOL, "dbcpService");
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.SQL_QUERY, "SELECT * FROM TEST_LOOKUP_DB WHERE name = :name");
        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlRecordLookupService);

        setupDB();
    }

    @Test
    public void testCorrectKeys() throws Exception {
        assertEquals(sqlRecordLookupService.getRequiredKeys(), Collections.emptySet());
    }

    @Test
    public void testCorrectValueType() throws Exception {
        assertEquals(sqlRecordLookupService.getValueType(), Record.class);
    }

    @Test
    public void testOnDisabled() throws Exception {
        sqlRecordLookupService.onDisabled();
        assertEquals(sqlRecordLookupService.cache.asMap().size(), 0);
    }

    @Test
    public void testSimpleLookup0() throws Exception {
        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", "547897511298456"));
        assertTrue(get1.isPresent());
        assertEquals("Consider the Lilies", get1.get().getAsString("VALUE"));
    }

    @Test
    public void testSimpleLookup1() throws Exception {
        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", "867142279069316"));
        assertTrue(get1.isPresent());
        assertEquals("The Needles Eye", get1.get().getAsString("VALUE"));
    }

    @Test
    public void testSimpleLookup2() throws Exception {
        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", "443771414357476"));
        assertTrue(get1.isPresent());
        assertEquals("Françoise Sagan", get1.get().getAsString("VALUE"));
    }

    @Test
    public void testEmptyLookup() throws Exception {
        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", ""));
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testInvalidLookup() throws Exception {
        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", "notavalue"));
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testRecordLookup() throws Exception {
        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", "443771414357476"));
        assertTrue(get1.isPresent());
        assertEquals("443771414357476", get1.get().getAsString("NAME"));
        assertEquals("Françoise Sagan", get1.get().getAsString("VALUE"));
        assertEquals(9, get1.get().getAsInt("PERIOD").intValue());
        assertEquals("96098 Walter Mall", get1.get().getAsString("ADDRESS"));
        assertEquals(24.67, get1.get().getAsDouble("PRICE"), 1.0);
    }

    @Test
    public void testArrayLookup() throws Exception {
        runner.disableControllerService(sqlRecordLookupService);
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.USE_JDBC_TYPES, "true");
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.SQL_QUERY, "SELECT array_agg(VALUE ORDER BY NAME DESC) AS LOTSA FROM TEST_LOOKUP_DB WHERE NAME IN(:name)");
        runner.assertValid(sqlRecordLookupService);
        runner.enableControllerService(sqlRecordLookupService);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", Arrays.asList(
                "990192861112958",
                "012470853914233",
                "912066265194017"
        ));
        String[] stringArray = {"Cabbages and Kings", "A Handful of Dust", "In Death Ground"};
        Object[] resultArray = DataTypeUtils.convertRecordArrayToJavaArray(stringArray, RecordFieldType.STRING.getDataType());

        final Optional<Record> get1 = sqlRecordLookupService.lookup(criteria);
        assertTrue(get1.isPresent());

        assertArrayEquals(resultArray, get1.get().getAsArray("LOTSA"));
    }

    @Test
    public void testExpressionLanguage() throws Exception {
        runner.disableControllerService(sqlRecordLookupService);
        runner.setProperty(sqlRecordLookupService, SQLLookupService.SQL_QUERY, "${literal(\"SELECT * FROM TEST_LOOKUP_DB WHERE name = \"):append(\":name\"):append(\";\")}");
        runner.assertValid(sqlRecordLookupService);
        runner.enableControllerService(sqlRecordLookupService);

        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", "443771414357476"));
        assertTrue(get1.isPresent());
        assertEquals("443771414357476", get1.get().getAsString("NAME"));
        assertEquals("Françoise Sagan", get1.get().getAsString("VALUE"));
        assertEquals(9, get1.get().getAsInt("PERIOD").intValue());
        assertEquals("96098 Walter Mall", get1.get().getAsString("ADDRESS"));
        assertEquals(24.67, get1.get().getAsDouble("PRICE"), 1.0);
    }

    @Test
    public void testNullLookup() throws Exception {
        final Optional<Record> get1 = sqlRecordLookupService.lookup(Collections.singletonMap("name", "is-a-null"));
        assertTrue(get1.isPresent());
        assertNull(get1.get().getAsString("VALUE"));
    }
}
