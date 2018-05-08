package org.apache.nifi.sqllookup;

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

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSQLNamedLoopupService extends AbstractSQLLookupServiceTest {

    static final Logger LOG = LoggerFactory.getLogger(TestSQLLookupService.class);

    private SQLLookupService sqlNamedLookupService;

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
        sqlNamedLookupService = new SQLLookupService();
        runner.addControllerService("SQLRecordLookupService", sqlNamedLookupService);
        runner.setProperty(sqlNamedLookupService, SQLLookupService.CONNECTION_POOL, "dbcpService");
        runner.setProperty(sqlNamedLookupService, SQLLookupService.SQL_QUERY, "SELECT * FROM TEST_LOOKUP_DB WHERE name = :name");
        runner.setProperty(sqlNamedLookupService, SQLLookupService.LOOKUP_VALUE_COLUMN, "VALUE");
        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlNamedLookupService);

        setupDB();
    }

    @Test
    public void testCorrectKeys() throws Exception {
        assertEquals(sqlNamedLookupService.getRequiredKeys(), singleton("key"));
    }

    @Test
    public void testCorrectValueType() throws Exception {
        assertEquals(sqlNamedLookupService.getValueType(), String.class);
    }

    @Test
    public void testSimpleLookup0() throws Exception {
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 547897511298456 }"));
        assertTrue(get1.isPresent());
        assertEquals("Consider the Lilies", get1.get());
    }

    @Test
    public void testSimpleLookup1() throws Exception {
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 867142279069316 }"));
        assertTrue(get1.isPresent());
        assertEquals("The Needles Eye", get1.get());
    }

    @Test
    public void testSimpleLookup2() throws Exception {
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 443771414357476 }"));
        assertTrue(get1.isPresent());
        assertEquals("Françoise Sagan", get1.get());
    }

    @Test
    public void testComplexLookup0() throws Exception {
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("key", "{ \"name\": 443771414357476, \"address\": \"96098 Walter Mall\" }"));
        assertTrue(get1.isPresent());
        assertEquals("Françoise Sagan", get1.get());
    }

    @Test
    public void testEmptyLookup() throws Exception {
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("key", "{\"name\": \"\"}"));
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testInvalidLookup() throws Exception {
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("key", "{ \"name\": \"notavalue\" }"));
        assertEquals(Optional.empty(), get1);
    }

    @Test
    public void testNullLookup() throws Exception {
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("key", "{ \"name\": \"is-a-null\" }"));
        assertEquals(Optional.empty(), get1);
    }
}
