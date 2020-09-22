package com.mrcsparker.nifi.sqllookup;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSQLLookupServiceInQuery extends AbstractSQLLookupServiceTest {

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
        runner.setProperty(sqlNamedLookupService, SQLLookupService.SQL_QUERY,
                        "SELECT * FROM TEST_LOOKUP_DB WHERE name IN (:name) ORDER BY name ASC");
        runner.setProperty(sqlNamedLookupService, SQLLookupService.LOOKUP_VALUE_COLUMN, "VALUE");
        runner.enableControllerService(dbcpService);
        runner.enableControllerService(sqlNamedLookupService);

        setupDB();
    }

    @Test
    public void testCorrectKeys() throws Exception {
        assertEquals(Collections.emptySet(), sqlNamedLookupService.getRequiredKeys());
    }

    @Test
    public void testCorrectValueType() throws Exception {
        assertEquals(sqlNamedLookupService.getValueType(), String.class);
    }

    @Test
    public void testSimpleLookup0() throws Exception {

        List<Long> names = Arrays.asList(547897511298456L, 867142279069316L);
        final Optional<String> get1 = sqlNamedLookupService.lookup(Collections.singletonMap("name", names));
        assertTrue(get1.isPresent());
        assertEquals("Consider the Lilies", get1.get());
    }
}
