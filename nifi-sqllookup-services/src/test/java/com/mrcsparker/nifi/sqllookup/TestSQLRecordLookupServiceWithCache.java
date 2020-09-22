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
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSQLRecordLookupServiceWithCache extends AbstractSQLLookupServiceTest {

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
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.SQL_QUERY,
                        "SELECT * FROM TEST_LOOKUP_DB WHERE name = :name");
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.CACHE_SIZE, "10");

        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlRecordLookupService);

        setupDB();
    }

    @Test
    public void testRecordLookup() throws Exception {

        assertEquals(sqlRecordLookupService.getCacheSize(), 0);

        for (int i = 0; i <= 10; i++) {
            final Optional<Record> get1 = sqlRecordLookupService
                            .lookup(Collections.singletonMap("name", "458006613841984"));
            assertTrue(get1.isPresent());
            assertEquals("458006613841984", get1.get().getAsString("NAME"));
            assertEquals("The Glory and the Dream", get1.get().getAsString("VALUE"));
            assertEquals(2, get1.get().getAsInt("PERIOD").intValue());
            assertEquals("84164 Gleason Branch", get1.get().getAsString("ADDRESS"));
            assertEquals(300.34, get1.get().getAsDouble("PRICE"), 1.0);

            assertEquals(sqlRecordLookupService.getCacheSize(), 1);
        }
    }

    @Test
    public void testRecordLookupMaxCaches() throws Exception {

        assertEquals(sqlRecordLookupService.getCacheSize(), 0);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "458006613841984"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 1);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "456148015917293"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 2);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "526924199146123"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 3);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "860683959429897"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 4);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "528661513839698"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 5);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "355663598958946"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 6);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "911753660676323"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 7);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "997417069743624"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 8);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "986873446696583"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 9);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "990409804141864"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 10);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "990409804141864"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 10);

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "990409804141864"));
        assertEquals(sqlRecordLookupService.getCacheSize(), 10);
    }

}
