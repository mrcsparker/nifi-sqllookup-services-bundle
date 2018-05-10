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
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSQLLookupService extends AbstractSQLLookupServiceTest {

    static final Logger LOG = LoggerFactory.getLogger(TestSQLLookupService.class);

    private DBCPService dbcpService = new DBCPServiceSimpleImpl();
    private SQLLookupService sqlLookupService;

    @Before
    public void before() throws Exception {
        TestProcessor testProcessor = new TestProcessor();
        runner = TestRunners.newTestRunner(testProcessor);

        // setup mock DBCP Service
        Map<String, String> dbcpProperties = new HashMap<>();

        runner.addControllerService("dbcpService", dbcpService, dbcpProperties);
        runner.assertValid(dbcpService);

        // setup SQLRecordLookupService
        sqlLookupService = new SQLLookupService();
        runner.addControllerService("SQLRecordLookupService", sqlLookupService);
        runner.setProperty(sqlLookupService, SQLLookupService.CONNECTION_POOL, "dbcpService");
        runner.setProperty(sqlLookupService, SQLLookupService.SQL_QUERY, "SELECT * FROM TEST_LOOKUP_DB WHERE name = :name");
        runner.setProperty(sqlLookupService, SQLLookupService.LOOKUP_VALUE_COLUMN, "VALUE");
        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlLookupService);

        setupDB();
    }

    @Test
    public void testCorrectKeys() throws Exception {
        assertEquals(sqlLookupService.getRequiredKeys(), Collections.emptySet());
    }

    @Test
    public void testCorrectValueType() throws Exception {
        assertEquals(sqlLookupService.getValueType(), String.class);
    }

    @Test
    public void testSimpleLookup0() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "547897511298456");

        final Optional<String> get1 = sqlLookupService.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("Consider the Lilies", get1.get());
    }

    @Test
    public void testSimpleLookup1() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "867142279069316");

        final Optional<String> get1 = sqlLookupService.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("The Needles Eye", get1.get());
    }

    @Test
    public void testSimpleLookup2() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "443771414357476");

        final Optional<String> get1 = sqlLookupService.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("Françoise Sagan", get1.get());
    }

    @Test
    public void testMultiValueLookup0() throws Exception {
        runner.disableControllerService(sqlLookupService);
        runner.setProperty(sqlLookupService, SQLLookupService.SQL_QUERY, "SELECT * FROM TEST_LOOKUP_DB WHERE name = :name AND address = :address");
        runner.assertValid(sqlLookupService);
        runner.enableControllerService(sqlLookupService);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "990192861112958");
        criteria.put("address", "84759 Jerrell Manors");

        final Optional<String> get1 = sqlLookupService.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("Cabbages and Kings", get1.get());
    }

    @Test
    public void testEmptyLookup() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "");

        final Optional<String> get1 = sqlLookupService.lookup(criteria);
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testInvalidLookup() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "notavalue");

        final Optional<String> get1 = sqlLookupService.lookup(criteria);
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testNullLookup() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "is-a-null");

        final Optional<String> get1 = sqlLookupService.lookup(criteria);
        assertEquals(Optional.empty(), get1);
    }

}
