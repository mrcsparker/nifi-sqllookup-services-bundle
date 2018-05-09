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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSQLLookupServiceWithCache extends AbstractSQLLookupServiceTest {
    private SQLLookupService sqlLookupService;

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
        sqlLookupService = new SQLLookupService();
        runner.addControllerService("SQLRecordLookupService", sqlLookupService);
        runner.setProperty(sqlLookupService, SQLLookupService.CONNECTION_POOL, "dbcpService");
        runner.setProperty(sqlLookupService, SQLLookupService.SQL_QUERY, "SELECT * FROM TEST_LOOKUP_DB WHERE name = ?");
        runner.setProperty(sqlLookupService, SQLLookupService.LOOKUP_VALUE_COLUMN, "VALUE");
        runner.setProperty(sqlLookupService, SQLLookupService.CACHE_SIZE, "10");
        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlLookupService);

        setupDB();
    }

    @Test
    public void testOnDisabled() throws Exception {
        sqlLookupService.onDisabled();
        assertEquals(sqlLookupService.cache.asMap().size(), 0);
    }

    @Test
    public void testRecordLookup() throws Exception {

        assertEquals(sqlLookupService.getCacheSize(), 0);

        for (int i = 0; i <= 10; i++) {
            final Optional<String> get1 = sqlLookupService.lookup(Collections.singletonMap("key", "458006613841984"));
            assertTrue(get1.isPresent());
            assertEquals("The Glory and the Dream", get1.get());

            assertEquals(sqlLookupService.getCacheSize(), 1);
        }
    }

    @Test
    public void testRecordLookupMaxCaches() throws Exception {

        assertEquals(sqlLookupService.getCacheSize(), 0);

        sqlLookupService.lookup(Collections.singletonMap("key", "458006613841984"));
        assertEquals(sqlLookupService.getCacheSize(), 1);

        sqlLookupService.lookup(Collections.singletonMap("key", "456148015917293"));
        assertEquals(sqlLookupService.getCacheSize(), 2);

        sqlLookupService.lookup(Collections.singletonMap("key", "526924199146123"));
        assertEquals(sqlLookupService.getCacheSize(), 3);

        sqlLookupService.lookup(Collections.singletonMap("key", "860683959429897"));
        assertEquals(sqlLookupService.getCacheSize(), 4);

        sqlLookupService.lookup(Collections.singletonMap("key", "528661513839698"));
        assertEquals(sqlLookupService.getCacheSize(), 5);

        sqlLookupService.lookup(Collections.singletonMap("key", "355663598958946"));
        assertEquals(sqlLookupService.getCacheSize(), 6);

        sqlLookupService.lookup(Collections.singletonMap("key", "911753660676323"));
        assertEquals(sqlLookupService.getCacheSize(), 7);

        sqlLookupService.lookup(Collections.singletonMap("key", "997417069743624"));
        assertEquals(sqlLookupService.getCacheSize(), 8);

        sqlLookupService.lookup(Collections.singletonMap("key", "986873446696583"));
        assertEquals(sqlLookupService.getCacheSize(), 9);

        sqlLookupService.lookup(Collections.singletonMap("key", "990409804141864"));
        assertEquals(sqlLookupService.getCacheSize(), 10);

        sqlLookupService.lookup(Collections.singletonMap("key", "990409804141864"));
        assertEquals(sqlLookupService.getCacheSize(), 10);
    }

    @Test
    public void testRecordLookupEmpty() throws Exception {
        Optional<String> key = sqlLookupService.lookup(Collections.singletonMap("key", "is-a-null"));
        assertFalse(key.isPresent());

        key = sqlLookupService.lookup(Collections.singletonMap("key", "is-a-null"));
        assertFalse(key.isPresent());
    }


}
