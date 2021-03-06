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

public class TestSQLRecordLookupServiceWithCache2k extends AbstractSQLLookupServiceTest {

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
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.CACHING_LIBRARY,
                        SQLRecordLookupService.CACHING_LIBRARY_CACHE2k);
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.CACHE_SIZE, "10");

        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlRecordLookupService);

        setupDB();
    }

    @Test
    public void testRecordLookup() throws Exception {

        assertEquals(0, sqlRecordLookupService.getCacheSize());

        for (int i = 0; i <= 10; i++) {
            final Optional<Record> get1 = sqlRecordLookupService
                            .lookup(Collections.singletonMap("name", "458006613841984"));
            assertTrue(get1.isPresent());
            assertEquals("458006613841984", get1.get().getAsString("NAME"));
            assertEquals("The Glory and the Dream", get1.get().getAsString("VALUE"));
            assertEquals(2, get1.get().getAsInt("PERIOD").intValue());
            assertEquals("84164 Gleason Branch", get1.get().getAsString("ADDRESS"));
            assertEquals(300.34, get1.get().getAsDouble("PRICE"), 1.0);

            assertEquals(1, sqlRecordLookupService.getCacheSize());
        }
    }

    @Test
    public void testRecordLookupMaxCaches() throws Exception {

        assertEquals(0, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "458006613841984"));
        assertEquals(1, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "456148015917293"));
        assertEquals(2, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "526924199146123"));
        assertEquals(3, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "860683959429897"));
        assertEquals(4, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "528661513839698"));
        assertEquals(5, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "355663598958946"));
        assertEquals(6, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "911753660676323"));
        assertEquals(7, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "997417069743624"));
        assertEquals(8, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "986873446696583"));
        assertEquals(9, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "990409804141864"));
        assertEquals(10, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "990409804141864"));
        assertEquals(10, sqlRecordLookupService.getCacheSize());

        sqlRecordLookupService.lookup(Collections.singletonMap("name", "990409804141864"));
        assertEquals(10, sqlRecordLookupService.getCacheSize());
    }

}
