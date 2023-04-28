/*
 * MIT License
 *
 * Copyright (c) 2022 FLATIDE LC.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.flatide.floodgate.system.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.agent.meta.MetaManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.Reader;
import java.sql.*;
import java.util.*;

import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

public class FDataSourceDB extends FDataSourceDefault {
    private static final Logger logger = LogManager.getLogger(MetaManager.class);

    private final JdbcTemplate jdbcTemplate;

    Connection connection = null;

    String url;
    String user;
    String password;
    Integer maxPoolSize;

    public FDataSourceDB(String name) throws Exception {
        super(name);

        this.url = ConfigurationManager.shared().getString("datasource." + name + ".url");
        this.user = ConfigurationManager.shared().getString("datasource." + name + ".user");
        this.password = ConfigurationManager.shared().getString("datasource." + name + ".password");
        this.maxPoolSize = ConfigurationManager.shared().getInteger("datasource." + name + ".maxPoolSize");

        DataSource dataSource = null;
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(this.url);
            config.setUsername(this.user);
            config.setPassword(this.password);
            config.setAutoCommit(false);
            config.setMaximumPoolSize(this.maxPoolSize);
            dataSource = new HikariDataSource(config);

        } catch( Exception e ) {
            e.printStackTrace();
            throw e;
        }

        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public boolean connect() throws Exception {
        return true;
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public String getName() {
        return "DB";
    }

    @Override
    public List<String> getAllKeys(String tableName, String keyColumn) {
        String query = "SELECT " + keyColumn + " FROM " + tableName;
        logger.debug(query);

        return jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement psmt = con.prepareStatement(query);

                return psmt;
            }
        }, new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getString(1);
            }
        });
    }

    @Override
    public boolean create(String key) {
        return false;
    }

    @Override
    public Map<String, Object> read(String tableName, String keyColumn, String key) throws Exception {
        String query = "SELECT * FROM " + tableName + " WHERE " + keyColumn + " = ?";
        logger.debug(query);

        return (Map<String, Object>) jdbcTemplate.queryForMap(query, new String[] {key});
    }

    @Override
    public List<Map<String, Object>> readList(String tableName, String keyColumn, String key) throws Exception {
        String query = "SELECT * FROM " + tableName;
        if( key != null && !key.isEmpty()) {
            query += " WHERE " + keyColumn + " like ?";
        }
        logger.debug(query);
        
        if( key != null && !key.isEmpty()) {
            return jdbcTemplate.queryForList(query, new String[] {"%" + key + "%"});
        } else {
            return jdbcTemplate.queryForList(query);
        }
    }

    @Override
    public boolean insert(String tableName, String keyColumn, Map<String, Object> row) throws Exception {
        List<String> colList = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
        query.append(tableName);
        query.append(" ( ");

        StringBuilder param = new StringBuilder();
        param.append(" ) VALUES ( ");

        int i = 0;
        for( String col : row.keySet() ) {
            if( i > 0 ) {
                query.append(", ");
            }
            query.append(col);
            colList.add(col);

            if( i > 0 ) {
                param.append(", ");
            }
            param.append("?");
            i++;
        }
        param.append(" ) ");
        query.append(param);

        logger.debug(query.toString());

        int count = jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement psmt = con.prepareStatement(query.toString());

                int i = 1;
                for (String col : colList) {
                    Object data = row.get(col);
                    if (data == null ) {
                        psmt.setNull(i++, Types.NULL);
                    } else {
                        if( data instanceof String) {
                            psmt.setString(i++, (String) data);
                        } else if (data instanceof Integer) {
                            psmt.setInt(i++, (Integer) data);
                        } else if (data instanceof java.sql.Date) {
                            psmt.setDate(i++, (java.sql.Date) data);
                        } else if( data instanceof java.sql.Time) {
                            psmt.setTime(i++, (java.sql.Time) data);
                        } else if( data instanceof java.sql.Timestamp) {
                            psmt.setTimestamp(i++, (java.sql.Timestamp) data);
                        } else {
                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                String json = mapper.writeValueAsString(data);
                                psmt.setString(i++, json);
                            } catch (Exception e) {
                                e.printStackTrace();
                                psmt.setObject(i++, data);
                            }
                        }
                    }
                }
                return psmt;
            }
        });
        return count != 0;
    }

    @Override
    public boolean update(String tableName, String keyColumn, Map<String, Object> row) throws Exception {
        List<String> colList = new ArrayList<>();

        String key = (String) row.remove(keyColumn);

        StringBuilder query = new StringBuilder();
        query.append("UPDATE ");
        query.append(tableName);
        query.append(" SET ");

        int i = 0;
        for( String col : row.keySet() ) {
            if( i > 0 ) {
                query.append(", ");
            }
            query.append( col );
            colList.add(col);
            query.append( " = ?");
            i++;
        }
        query.append(" WHERE ID = ? ");

        logger.debug(query.toString());
        int count = jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement psmt = con.prepareStatement(query.toString());

                int i = 1;

                for(String col : colList ) {
                    Object data = row.get(col);
                    if( data == null ) {
                        psmt.setNull(i++, Types.NULL);
                    } else {
                        if( data instanceof String ) {
                            psmt.setString(i++, (String) data);
                        } else if( data instanceof Integer) {
                            psmt.setInt(i++, (Integer) data);
                        } else if( data instanceof java.sql.Date) {
                            psmt.setDate(i++, (java.sql.Date) data);
                        } else if( data instanceof java.sql.Time) {
                            psmt.setTime(i++, (java.sql.Time) data);
                        } else if( data instanceof java.sql.Timestamp) {
                            psmt.setTimestamp(i++, (java.sql.Timestamp) data);
                        } else {
                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                String json = mapper.writeValueAsString(data);
                                psmt.setString(i++, json);
                            } catch (Exception e) {
                                psmt.setObject(i++, data);
                            }
                        }
                    }
                }

                return psmt;
            }
        });

        return count != 0;
    }

    @Override
    public boolean delete(String tableName, String keyColumn, String key) throws Exception {
        StringBuilder query = new StringBuilder();
        query.append("DELETE FROM ")
            .append(tableName)
            .append(" WHERE ")
            .append(keyColumn)
            .append(" = ?" );

        int count = jdbcTemplate.update(query.toString(), key);
        return count != 0;
    }

    @Override
    public int deleteAll() {
        return 0;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        try {
            if( this.connection != null ) {
                this.connection.close();
                this.connection = null;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
