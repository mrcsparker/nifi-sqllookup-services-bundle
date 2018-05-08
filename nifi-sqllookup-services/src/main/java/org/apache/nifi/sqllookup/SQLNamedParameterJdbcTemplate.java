package org.apache.nifi.sqllookup;

import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import javax.sql.DataSource;

public class SQLNamedParameterJdbcTemplate extends NamedParameterJdbcTemplate {
    public SQLNamedParameterJdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }

    public PreparedStatementCreator getPreparedStatement(String sql, SqlParameterSource paramSource) {
        return getPreparedStatementCreator(sql, paramSource);
    }
}
