package com.mrcsparker.nifi.sqllookup;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.standard.LookupRecord;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSQLRecordLookupServiceWithSchema extends AbstractSQLLookupServiceTest {

    static final Logger LOG = LoggerFactory.getLogger(TestSQLRecordLookupServiceWithSchema.class);

    private SQLRecordLookupService sqlRecordLookupService;

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(LookupRecord.class);
        runner.setProperty("period", "/period");

        final String inputSchemaText = new String(
                        Files.readAllBytes(Paths.get("src/test/resources/period-names.avsc")));
        JsonTreeReader recordReader = new JsonTreeReader();
        runner.addControllerService("reader", recordReader);
        runner.setProperty(recordReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY,
                        SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(recordReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.assertValid(recordReader);
        runner.enableControllerService(recordReader);

        final String outputSchemaText = new String(
                        Files.readAllBytes(Paths.get("src/test/resources/period-names.avsc")));
        JsonRecordSetWriter recordWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(recordWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY,
                        SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(recordWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(recordWriter, "Pretty Print JSON", "true");
        runner.setProperty(recordWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.assertValid(recordWriter);
        runner.enableControllerService(recordWriter);

        // setup mock DBCP Service
        DBCPService dbcpService = new DBCPServiceSimpleImpl();
        Map<String, String> dbcpProperties = new HashMap<>();

        runner.addControllerService("dbcpService", dbcpService, dbcpProperties);
        runner.assertValid(dbcpService);
        runner.enableControllerService(dbcpService);

        // setup SQL Record Lookup Service
        sqlRecordLookupService = new SQLRecordLookupService();
        runner.addControllerService("SQLRecordLookupService", sqlRecordLookupService);
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.CONNECTION_POOL, "dbcpService");
        runner.setProperty(sqlRecordLookupService, SQLRecordLookupService.SQL_QUERY,
                        "SELECT array_agg(VALUE ORDER BY NAME DESC) AS names FROM TEST_LOOKUP_DB WHERE PERIOD IN(:period)");
        runner.assertValid(sqlRecordLookupService);
        runner.enableControllerService(sqlRecordLookupService);

        setupDB();
    }

    @Test
    public void testCorrectKeys() throws Exception {
        assertEquals(Collections.emptySet(), sqlRecordLookupService.getRequiredKeys());
    }

    @Test
    public void testCorrectValueType() throws Exception {
        assertEquals(sqlRecordLookupService.getValueType(), Record.class);
    }
}
