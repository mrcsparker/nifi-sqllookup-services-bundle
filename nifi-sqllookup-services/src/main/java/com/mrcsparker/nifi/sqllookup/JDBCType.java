package com.mrcsparker.nifi.sqllookup;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class JDBCType {

    static final Logger LOG = LoggerFactory.getLogger(JDBCType.class);

    static DataType getType(int sqlType) {
        switch (sqlType) {
            case Types.ARRAY:
                return RecordFieldType.ARRAY.getDataType();
            case Types.BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case Types.DATE:
                return RecordFieldType.DATE.getDataType();
            case Types.DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case Types.FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return RecordFieldType.INT.getDataType();
            case Types.TIME: return RecordFieldType.TIME.getDataType();
            case Types.TIMESTAMP:
                return RecordFieldType.TIMESTAMP.getDataType();
            case Types.CHAR:
            case Types.VARCHAR:
                return RecordFieldType.STRING.getDataType();
        }
        throw new UnsupportedOperationException("SQL type " + sqlType + " not supported.");
    }

    static Object getValue(ResultSet resultSet, int sqlType, int column) throws SQLException {
        switch (sqlType) {
            case Types.ARRAY:
                return resultSet.getArray(column).getArray();
            case Types.BOOLEAN:
                return resultSet.getBoolean(column);
            case Types.DATE:
                return resultSet.getDate(column);
            case Types.DOUBLE:
                return resultSet.getDouble(column);
            case Types.FLOAT:
                return resultSet.getFloat(column);
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return resultSet.getInt(column);
            case Types.TIME: return RecordFieldType.TIME.getDataType();
            case Types.TIMESTAMP:
                return resultSet.getTimestamp(column);
            case Types.CHAR:
            case Types.VARCHAR:
                return resultSet.getString(column);
        }
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
