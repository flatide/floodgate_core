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

public class FDataSourceDB extends FDataSourceDefault {
    private static final Logger logger = LogManager.getLogger(MetaManager.class);

    Connection connection = null;

    String url;
    String user;
    String password;
    Integer maxPoolSize;

    public FDataSourceDB(String name) {
        super(name);

        this.url = ConfigurationManager.shared().getString("datasource." + name + ".url");
        this.user = ConfigurationManager.shared().getString("datasource." + name + ".user");
        this.password = ConfigurationManager.shared().getString("datasource." + name + ".password");
        this.maxPoolSize = ConfigurationManager.shared().getInteger("datasource." + name + ".maxPoolSize");
    }

    @Override
    public boolean connect() throws Exception {
        DataSource dataSource = null;
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(this.url);
            config.setUsername(this.user);
            config.setPassword(this.password);
            config.setAutoCommit(false);
            config.setMaximumPoolSize(this.maxPoolSize);
            dataSource = new HikariDataSource(config);

            this.connection = dataSource.getConnection();
            //this.connection = DriverManager.getConnection(this.url, this.user, this.password);
            return true;
        } catch( Exception e ) {
            e.printStackTrace();
            throw e;
        }
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

        ArrayList<String> result = new ArrayList<>();
        try ( PreparedStatement ps = this.connection.prepareStatement(query); ResultSet rs = ps.executeQuery() ){
            //ResultSetMetaData rsmeta = rs.getMetaData();

            while(rs.next()) {
                String key = rs.getString(1);
                result.add(key);
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public boolean create(String key) {
        return false;
    }

    @Override
    public Map<String, Object> read(String tableName, String keyColumn, String key) throws Exception {
        String query = "SELECT * FROM " + tableName + " WHERE " + keyColumn + " = ?";
        logger.debug(query);

        try (PreparedStatement ps = this.connection.prepareStatement(query) ){
            ps.setString(1, key);

            try (ResultSet rs = ps.executeQuery() ) {

                if (rs.next()) {
                    Map<String, Object> row = new HashMap<>();

                    ResultSetMetaData rsmeta = rs.getMetaData();
                    int count = rsmeta.getColumnCount();
                    for( int i = 1; i <= count; i++ ) {
                        String name = rsmeta.getColumnName(i);
                        Object obj = rs.getObject(i);

                        row.put(name, obj);
                    }
                    return row;
                }
            }
            return null;
        } catch(SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public List<Map<String, Object>> readList(String tableName, String keyColumn, String key) throws Exception {
        String query = "SELECT * FROM " + tableName;
        if( key != null && !key.isEmpty()) {
            query += " WHERE " + keyColumn + " like ?";
        }
        logger.debug(query);

        List<Map<String, Object>> result = new ArrayList<>();

        try( PreparedStatement ps = this.connection.prepareStatement(query) ) {
            if( key != null && !key.isEmpty()) {
                ps.setString(1, "%" + key + "%");
            }

            try (ResultSet rs = ps.executeQuery() ) {
                
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();

                    ResultSetMetaData rsmeta = rs.getMetaData();
                    int count = rsmeta.getColumnCount();
                    for( int i = 1; i <= count; i++) {
                        String name = rsmeta.getColumnName(i);
                        row.put(name, rs.getObject(i));
                    }

                    result.add(row);
                }
            }

            return result;
        } catch(SQLException e) {
            e.printStackTrace();
            throw e;
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

        try (PreparedStatement ps = this.connection.prepareStatement(query.toString())) {
            i = 1;
            for(String col : colList ) {
                Object data = row.get(col);
                if( data == null ) {
                    ps.setNull(i++, Types.NULL);
                } else {
                    if( data instanceof String ) {
                        ps.setString(i++, (String) data);
                    } else if( data instanceof Integer) {
                        ps.setInt(i++, (Integer) data);
                    } else if( data instanceof java.sql.Date) {
                        ps.setDate(i++, (java.sql.Date) data);
                    } else if( data instanceof java.sql.Time) {
                        ps.setTime(i++, (java.sql.Time) data);
                    } else if( data instanceof java.sql.Timestamp) {
                        ps.setTimestamp(i++, (java.sql.Timestamp) data);
                    } else {
                        ObjectMapper mapper = new ObjectMapper();
                        String json = mapper.writeValueAsString(data);

                        ps.setString(i++, json);
                    }
                }
            }

            ps.executeUpdate();

            this.connection.commit();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
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

        try (PreparedStatement ps = this.connection.prepareStatement(query.toString())) {
            i = 1;
            for(String col : colList ) {
                Object data = row.get(col);
                if( data == null ) {
                    ps.setNull(i++, Types.NULL);
                } else {
                    if( data instanceof String ) {
                        ps.setString(i++, (String) data);
                    } else if( data instanceof Integer) {
                        ps.setInt(i++, (Integer) data);
                    } else if( data instanceof java.sql.Date) {
                        ps.setDate(i++, (java.sql.Date) data);
                    } else if( data instanceof java.sql.Time) {
                        ps.setTime(i++, (java.sql.Time) data);
                    } else if( data instanceof java.sql.Timestamp) {
                        ps.setTimestamp(i++, (java.sql.Timestamp) data);
                    } else {
                        ObjectMapper mapper = new ObjectMapper();
                        String json = mapper.writeValueAsString(data);

                        ps.setString(i++, json);
                    }
                }
            }
            ps.setString(i, key);

            int count = ps.executeUpdate();
            
            this.connection.commit();

            return count != 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public boolean delete(String tableName, String keyColumn, String key) throws Exception {
        StringBuilder query = new StringBuilder();
        query.append("DELETE FROM ")
            .append(tableName)
            .append(" WHERE ")
            .append(keyColumn)
            .append(" = ?" );

        try (PreparedStatement ps = this.connection.prepareStatement(query.toString())) {
            ps.setQueryTimeout(0);

            ps.setString(1, key);

            ps.execute();

            this.connection.commit();

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
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
