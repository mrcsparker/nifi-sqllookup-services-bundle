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
package org.apache.nifi.sqllookup;

// Version of https://github.com/apache/nifi/blob/master/nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/ResultSetRecordSet.java
// adapted for lookup

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;

import java.math.BigInteger;
import java.sql.*;
import java.util.*;

class SQLRecordSet {

    private final ResultSet rs;
    private final Set<String> rsColumnNames;

    private final RecordSchema schema;

    SQLRecordSet(final ResultSet rs) throws SQLException {
        this.rs = rs;

        rsColumnNames = new HashSet<>();
        final ResultSetMetaData metadata = rs.getMetaData();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            rsColumnNames.add(metadata.getColumnLabel(i + 1));
        }

        schema = createSchema(rs);
    }

    public Optional<Record> getMapRecord() throws SQLException {
        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());

        for (final RecordField field : schema.getFields()) {
            final String fieldName = field.getFieldName();

            final Object value;
            if (rsColumnNames.contains(fieldName)) {
                value = normalizeValue(rs.getObject(fieldName));
            } else {
                value = null;
            }

            values.put(fieldName, value);
        }

        return Optional.of(new MapRecord(schema, values));
    }

    @SuppressWarnings("rawtypes")
    private Object normalizeValue(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof List) {
            return ((List) value).toArray();
        }

        return value;
    }

    private static RecordSchema createSchema(final ResultSet rs) throws SQLException {
        final ResultSetMetaData metadata = rs.getMetaData();
        final int numCols = metadata.getColumnCount();
        final List<RecordField> fields = new ArrayList<>(numCols);

        for (int i = 0; i < numCols; i++) {
            final int column = i + 1;
            final int sqlType = metadata.getColumnType(column);

            final DataType dataType = getDataType(sqlType, rs, column);
            final String fieldName = metadata.getColumnLabel(column);

            final int nullableFlag = metadata.isNullable(column);
            final RecordField field = new RecordField(fieldName, dataType, nullableFlag != ResultSetMetaData.columnNoNulls);
            fields.add(field);
        }

        return new SimpleRecordSchema(fields);
    }

    private static DataType getDataType(final int sqlType, final ResultSet rs, final int columnIndex) throws SQLException {
        switch (sqlType) {
            case Types.ARRAY:
                // The JDBC API does not allow us to know what the base type of an array is through the metadata.
                // As a result, we have to obtain the actual Array for this record. Once we have this, we can determine
                // the base type. However, if the base type is, itself, an array, we will simply return a base type of
                // String because otherwise, we need the ResultSet for the array itself, and many JDBC Drivers do not
                // support calling Array.getResultSet() and will throw an Exception if that is not supported.
                if (rs.isAfterLast()) {
                    return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
                }

                final Array array = rs.getArray(columnIndex);
                if (array == null) {
                    return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
                }

                final DataType baseType = getArrayBaseType(array);
                return RecordFieldType.ARRAY.getArrayDataType(baseType);
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case Types.OTHER:
                return RecordFieldType.RECORD.getDataType();
            default:
                return getFieldType(sqlType).getDataType();
        }
    }

    private static DataType getArrayBaseType(final Array array) throws SQLException {
        final Object arrayValue = array.getArray();
        if (arrayValue == null) {
            return RecordFieldType.STRING.getDataType();
        }

        if (arrayValue instanceof byte[]) {
            return RecordFieldType.BYTE.getDataType();
        }
        if (arrayValue instanceof int[]) {
            return RecordFieldType.INT.getDataType();
        }
        if (arrayValue instanceof long[]) {
            return RecordFieldType.LONG.getDataType();
        }
        if (arrayValue instanceof boolean[]) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        if (arrayValue instanceof short[]) {
            return RecordFieldType.SHORT.getDataType();
        }
        if (arrayValue instanceof byte[]) {
            return RecordFieldType.BYTE.getDataType();
        }
        if (arrayValue instanceof float[]) {
            return RecordFieldType.FLOAT.getDataType();
        }
        if (arrayValue instanceof double[]) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (arrayValue instanceof char[]) {
            return RecordFieldType.CHAR.getDataType();
        }
        if (arrayValue instanceof Object[]) {
            final Object[] values = (Object[]) arrayValue;
            if (values.length == 0) {
                return RecordFieldType.STRING.getDataType();
            }

            Object valueToLookAt = null;
            for (Object value : values) {
                valueToLookAt = value;
                if (valueToLookAt != null) {
                    break;
                }
            }
            if (valueToLookAt == null) {
                return RecordFieldType.STRING.getDataType();
            }

            if (valueToLookAt instanceof String) {
                return RecordFieldType.STRING.getDataType();
            }
            if (valueToLookAt instanceof Long) {
                return RecordFieldType.LONG.getDataType();
            }
            if (valueToLookAt instanceof Integer) {
                return RecordFieldType.INT.getDataType();
            }
            if (valueToLookAt instanceof Short) {
                return RecordFieldType.SHORT.getDataType();
            }
            if (valueToLookAt instanceof Byte) {
                return RecordFieldType.BYTE.getDataType();
            }
            if (valueToLookAt instanceof Float) {
                return RecordFieldType.FLOAT.getDataType();
            }
            if (valueToLookAt instanceof Double) {
                return RecordFieldType.DOUBLE.getDataType();
            }
            if (valueToLookAt instanceof Boolean) {
                return RecordFieldType.BOOLEAN.getDataType();
            }
            if (valueToLookAt instanceof Character) {
                return RecordFieldType.CHAR.getDataType();
            }
            if (valueToLookAt instanceof BigInteger) {
                return RecordFieldType.BIGINT.getDataType();
            }
            if (valueToLookAt instanceof Integer) {
                return RecordFieldType.INT.getDataType();
            }
            if (valueToLookAt instanceof java.sql.Time) {
                return RecordFieldType.TIME.getDataType();
            }
            if (valueToLookAt instanceof java.sql.Date) {
                return RecordFieldType.DATE.getDataType();
            }
            if (valueToLookAt instanceof java.sql.Timestamp) {
                return RecordFieldType.TIMESTAMP.getDataType();
            }
            if (valueToLookAt instanceof Record) {
                final Record record = (Record) valueToLookAt;
                return RecordFieldType.RECORD.getRecordDataType(record.getSchema());
            }
        }

        return RecordFieldType.STRING.getDataType();
    }


    private static RecordFieldType getFieldType(final int sqlType) {
        switch (sqlType) {
            case Types.BIGINT:
            case Types.ROWID:
                return RecordFieldType.LONG;
            case Types.BIT:
            case Types.BOOLEAN:
                return RecordFieldType.BOOLEAN;
            case Types.CHAR:
                return RecordFieldType.CHAR;
            case Types.DATE:
                return RecordFieldType.DATE;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.REAL:
                return RecordFieldType.DOUBLE;
            case Types.FLOAT:
                return RecordFieldType.FLOAT;
            case Types.INTEGER:
                return RecordFieldType.INT;
            case Types.SMALLINT:
                return RecordFieldType.SHORT;
            case Types.TINYINT:
                return RecordFieldType.BYTE;
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NULL:
            case Types.NVARCHAR:
            case Types.VARCHAR:
                return RecordFieldType.STRING;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
                return RecordFieldType.RECORD;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return RecordFieldType.TIME;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return RecordFieldType.TIMESTAMP;
        }

        return RecordFieldType.STRING;
    }

}
