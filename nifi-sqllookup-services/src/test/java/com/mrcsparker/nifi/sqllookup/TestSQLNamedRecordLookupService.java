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

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestSQLNamedRecordLookupService extends AbstractSQLLookupServiceTest {
    private SQLRecordLookupService sqlNamedRecordLookupService;

    @Before
    public void before() throws Exception {
        TestProcessor testProcessor = new TestProcessor();
        runner = TestRunners.newTestRunner(testProcessor);

        // setup mock DBCP Service
        DBCPService dbcpService = new AbstractSQLLookupServiceTest.DBCPServiceSimpleImpl();
        Map<String, String> dbcpProperties = new HashMap<>();

        runner.addControllerService("dbcpService", dbcpService, dbcpProperties);
        runner.assertValid(dbcpService);

        // setup SQLRecordLookupService
        sqlNamedRecordLookupService = new SQLRecordLookupService();
        runner.addControllerService("SQLRecordLookupService", sqlNamedRecordLookupService);
        runner.setProperty(sqlNamedRecordLookupService, SQLRecordLookupService.CONNECTION_POOL, "dbcpService");
        runner.setProperty(sqlNamedRecordLookupService, SQLRecordLookupService.SQL_QUERY, "SELECT * FROM TEST_LOOKUP_DB WHERE name = :name");
        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlNamedRecordLookupService);

        setupDB();
    }

    @Test
    public void testCorrectKeys() throws Exception {
        assertEquals(sqlNamedRecordLookupService.getRequiredKeys(), singleton("key"));
    }

    @Test
    public void testCorrectValueType() throws Exception {
        assertEquals(sqlNamedRecordLookupService.getValueType(), Record.class);
    }

    @Test
    public void testOnDisabled() throws Exception {
        sqlNamedRecordLookupService.onDisabled();
        assertEquals(sqlNamedRecordLookupService.cache.asMap().size(), 0);
    }

    @Test
    public void testSimpleLookup0() throws Exception {
        final Optional<Record> get1 = sqlNamedRecordLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 547897511298456 }"));
        assertTrue(get1.isPresent());
        assertEquals("Consider the Lilies", get1.get().getAsString("VALUE"));
    }

    @Test
    public void testSimpleLookup1() throws Exception {
        final Optional<Record> get1 = sqlNamedRecordLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 867142279069316 }"));
        assertTrue(get1.isPresent());
        assertEquals("The Needles Eye", get1.get().getAsString("VALUE"));
    }

    @Test
    public void testSimpleLookup2() throws Exception {
        final Optional<Record> get1 = sqlNamedRecordLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 443771414357476 }"));
        assertTrue(get1.isPresent());
        assertEquals("Françoise Sagan", get1.get().getAsString("VALUE"));
    }

    @Test
    public void testEmptyLookup() throws Exception {
        final Optional<Record> get1 = sqlNamedRecordLookupService.lookup(Collections.singletonMap("key", "{ \"name\": \"\" }"));
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testInvalidLookup() throws Exception {
        final Optional<Record> get1 = sqlNamedRecordLookupService.lookup(Collections.singletonMap("key", "{ \"name\": \"notavalue\" }"));
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testRecordLookup() throws Exception {
        final Optional<Record> get1 = sqlNamedRecordLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 443771414357476 }"));
        assertTrue(get1.isPresent());
        assertEquals("443771414357476", get1.get().getAsString("NAME"));
        assertEquals("Françoise Sagan", get1.get().getAsString("VALUE"));
        assertEquals(9, get1.get().getAsInt("PERIOD").intValue());
        assertEquals("96098 Walter Mall", get1.get().getAsString("ADDRESS"));
        assertEquals(24.67, get1.get().getAsDouble("PRICE"), 1.0);
    }

    @Test
    public void testNullLookup() throws Exception {
        final Optional<Record> get1 = sqlNamedRecordLookupService.lookup(Collections.singletonMap("key", "{ \"name\": \"is-a-null\" }"));
        assertTrue(get1.isPresent());
        assertNull(get1.get().getAsString("VALUE"));
    }
}
