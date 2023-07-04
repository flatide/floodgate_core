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

package com.flatide.floodgate.agent.connector;

import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.flow.rule.MappingRuleItem;
import com.flatide.floodgate.agent.handler.HandlerManager;
import com.flatide.floodgate.agent.handler.HandlerManager.Step;
import com.flatide.floodgate.agent.template.DocumentTemplate;
import com.flatide.floodgate.agent.flow.rule.MappingRule;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.flow.rule.FunctionProcessor;
import com.flatide.floodgate.system.FlowEnv;
import com.flatide.floodgate.system.security.FloodgateSecurity;

//import com.zaxxer.hikari.HikariConfig;
//import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConnectorDB extends ConnectorBase {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    private static final Logger logger = LogManager.getLogger(ConnectorDB.class);

    private static final ConcurrentHashMap<String, DataSource> pools = new ConcurrentHashMap<>();

    private Connection connection = null;

    private String query = "";
    private List<String> param;

    private int sent = 0;

    private long cur;

    private static class DBFunctionProcessorMySql implements FunctionProcessor {
        public Object process(MappingRuleItem item) {
            String func = item.getSourceName();
            MappingRule.Function function = MappingRule.Function.valueOf(func);

            switch (function) {
                case TARGET_DATE:
                    //item.setAction(MappingRuleItem.RuleAction.literal);
                    return "now()";
                default:
                    return "?";
            }
        }
    }

    private static class DBFunctionProcessorOracle implements FunctionProcessor {
        public Object process(MappingRuleItem item) {
            String func = item.getSourceName();
            MappingRule.Function function = MappingRule.Function.valueOf(func);

            switch (function) {
                case TARGET_DATE:
                    //item.setAction(MappingRuleItem.RuleAction.literal);
                    return "sysdate";
                default:
                    return "?";
            }
        }
    }

    static FunctionProcessor PRE_PROCESSOR_ORACLE = new DBFunctionProcessorOracle();
    static FunctionProcessor PRE_PROCESSOR_MYSQL = new DBFunctionProcessorMySql();

    Object processEmbedFunction(String function) {
        switch( function ) {
            case "DATE":
                long current = System.currentTimeMillis();
                return new Date(current);
            case "SEQ":
                return FlowEnv.shared().getSequence();
            default:
                return null;
        }
    }

    @Override
    public FunctionProcessor getFunctionProcessor(String type) {
        if( type == null ) {
            return PRE_PROCESSOR_MYSQL;
        }
        switch(type.toUpperCase()) {
            case "ORACLE":
                return PRE_PROCESSOR_ORACLE;
            default:
                return PRE_PROCESSOR_MYSQL;
        }
    }

    @Override
    public void connect(Context context, Module module) throws Exception {
        cur = System.currentTimeMillis();

        super.connect(context, module);

        /*if( this.name != null && !this.name.isEmpty() ) {
            DataSource dataSource = pools.get(this.name);
            if( dataSource == null ) {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(this.url);
                config.setUsername(this.user);
                config.setPassword(this.password);
                dataSource = new HikariDataSource(config);
                pools.put(this.name, dataSource);
            }
            this.connection = dataSource.getConnection();
        } else*/ {
            this.password = FloodgateSecurity.shared().decrypt(this.password);
            this.connection = DriverManager.getConnection(this.url, this.user, this.password);
        }

        this.connection.setAutoCommit(false);

        DatabaseMetaData meta = this.connection.getMetaData();
        logger.debug(meta.getDatabaseProductName() + " : " + meta.getDatabaseProductVersion());
    }

    /*@Override
    public void beforeCreate(MappingRule mappingRule) throws Exception {
        //DocumentTemplate documentTemplate = getDocumentTemplate();

        //query = documentTemplate.makeHeader(this.output, mappingRule);
        //query = query + documentTemplate.makeFooter(this.output, mappingRule);

        //super.beforeCreate(mappingRule);
    }*/

    @Override
    public int creating(List<Map<String, Object>> itemList, MappingRule mappingRule, long index, int batchSize) throws Exception {
        Context channelContext = (Context) context.get(Context.CONTEXT_KEY.CHANNEL_CONTEXT);
        if( this.query.isEmpty()) {
            DocumentTemplate documentTemplate = getDocumentTemplate();

            List<Map<String, Object>> temp = new ArrayList<>();
            Map<String, Object> one = itemList.get(0);
            Map<String, Object> copy = new HashMap<>();
            for( Map.Entry<String, Object> e : one.entrySet() ) {
                copy.put(e.getKey(), "?");
            }
            temp.add(copy);

            this.query = documentTemplate.makeHeader(context, mappingRule, temp);
            //this.query += documentTemplate.makeBody(this.output, mappingRule);
            this.param = mappingRule.getParam();
            logger.debug(this.query);
        }
        //cur = System.currentTimeMillis();
        try (PreparedStatement ps = this.connection.prepareStatement(this.query)) {
            try {
                int count = 0;
                int timeout = this.context.getIntegerDefault("SEQUENCE.TIMEOUT", 0);
                for (Map item : itemList) {
                    int i = 1;
                    for (String key : this.param) {
                        if( key.startsWith(">")) {
                            Object value = processEmbedFunction(key.substring(1));
                            ps.setObject(i++, value);
                        } else if( key.startsWith("{")) {
                            Object value = context.get(key.substring(1, key.length() - 1 ));
                            ps.setObject(i++, value);
                        } else {
                            ps.setObject(i++, item.get(key));
                        }
                    }

                    ps.addBatch();
                    count++;
                    if( count >= batchSize ) {
                        ps.setQueryTimeout(timeout);
                        cur = System.currentTimeMillis();
                        ps.executeBatch();
                        //this.connection.commit();
                        this.sent += count;
                        count = 0;

                        this.module.setProgress(this.sent);
                        HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
                        logger.debug(System.currentTimeMillis() - this.cur);
                    }

                    /*ps.executeUpdate();
                    this.connection.commit();
                    this.sent++;
                    System.out.println(System.currentTimeMillis() - this.cur);
                     */
                }
                if( count > 0 ) {
                    ps.setQueryTimeout(timeout);
                    cur = System.currentTimeMillis();
                    ps.executeBatch();
                    //this.connection.commit();
                    this.sent += count;
                    this.module.setProgress(this.sent);
                    HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
                    logger.debug(System.currentTimeMillis() - this.cur);
                }
                //this.connection.commit();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        return sent;
    }

    @Override
    public void commit() throws SQLException {
        this.connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        this.sent = 0;
        this.connection.rollback();
        Context channelContext = (Context) contexts.get(Context.CONTEXT_KEY.CHANNEL_CONTEXT);
        this.module.setProgress(0);
        HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
    }

    @Override
    public List<Map> read(MappingRule rule) throws Exception {
        Context channelContext = (Context) context.get(Context.CONTEXT_KEY.CHANNEL_CONTEXT);

        String table = (String) this.context.get("SEQUENCE.TARGET");
        String sql = (String) this.context.get("SEQUENCE.SQL");
        String condition = (String) this.context.getString("SEQUENCE.CONDITION");

        String query = "";

        if( sql != null ) {
            query = sql;
        } else {
            query = "SELECT ";
            Set<String> sourceSet = new LinkedHashSet<>();

            for (MappingRuleItem item : rule.getRules()) {
                switch(item.getAction()) {
                case system:
                    break;
                case function:
                    break;
                case literal:
                    // it is possible that source column is exist in literal 
                    String source = item.getSourceName();

                    Pattern pattern = Pattern.compile("\\$.+?\\$");
                    Matcher matcher = pattern.matcher(source);

                    while (matcher.find()) {
                        String col = source.substring(matcher.start() + 1, matcher.end() - 1);
                        sourceSet.add(col);
                    }
                    break;
                case reference:
                    sourceSet.add(item.getSourceName());
                    // source column
                    break;
                case order:
                    break;
                default:
                    break;
                }
            }

            int i = 0;
            for (String source : sourceSet) {
                if( i > 0 ) {
                    query += ", ";
                }
                query += source;
                i++;
            }

            query += " FROM " + table;
            if (condition != null && !condition.isEmpty()) {
                query += " WHERE " + condition;
            }
            logger.debug(query);
        }

        Integer fetchSize = this.context.setIntegerDefault("SEQUENCE.FETCHSIZE", 0);
        Integer sizeForUpdateHandler = fetchSize < 1000 ? 3000 : fetchSize * 3;
        Boolean flush = (Boolean) this.context.getDefault("SEQUENCE.FLUSH", Boolean.valueOf(false)); 
        List<Map> result = new ArrayList<>();
        try (PreparedStatement ps = this.connection.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (fetchSize > 0 ) {
                rs.setFetchSize(fetchSize);
            }
            ResultSetMetaData rsmeta = rs.getMetaData();

            int count = rsmeta.getColumnCount();
            int c = 0, retrieve = 0;
            while (rs.next()) {
                if (!flush) {
                    Map<String, Object> column = new LinkedHashMap<>();
                    for (int i = 1; i <= count; i++) {
                        Object row = rs.getObject(i);

                        if (row instanceof oracle.sql.TIMESTAMP) {
                            // Jackson cannot (de)serialize oracle.sql.TIMESTAMP, converting it to java.sql.Timestamp
                            row = ((oracle.sql.TIMESTAMP)row).timestampValue();
                        }
                        // TODO process Clob and skip Blob
                        column.put(rsmeta.getColumnLabel(i), row);
                    }

                    result.add(column);
                }
                
                c++;
                if (c >= sizeForUpdateHandler) {
                    retrieve += c;
                    c = 0;
                    this.module.setProgress(retrieve);
                    HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
                }
            }
            if (c>0) {
                retrieve += c;
                this.module.setProgress(retrieve);
                HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
            }
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }

        return result;
    }

    @Override
    public void check() throws Exception {
        String table = (String) this.context.get("SEQUENCE.TARGET");

        DatabaseMetaData databaseMetaData = this.connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getTables(null, null, table, new String[] {"TABLE"});
        boolean exist = resultSet.next();

        if (!exist) {
            throw new Exception(table + " is not exist.");
        }
    }

    @Override
    public void count() throws Exception {
        Context channelContext = (Context) context.get(Context.CONTEXT_KEY.CHANNEL_CONTEXT);

        Stirng table = (String) this.context.get("SEQUENCE.TARGET");
        String sql = (String) this.context.get("SEQUENCE.SQL");
        String condition = (String) this.context.get("SEQUENCE.CONDITION");

        String query = "";

        if (sql != null) {
            query = sql;
        } else {
            query = "SELECT COUNT(*) AS COUNT";
            query += " FROM " + table;
            if (condition != null && !condition.isEmpty()) {
                query += " WHERE " + condition;
            }
            logger.debug(query);
        }

        try (PreparedStatement ps = this.conneciton.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int count = rs.getInt("COUNT");
                this.module.setProgress(count);
                HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
            }
        } catch (Excepiton e) {
            e.printStackTrace();
            throw e;
        }
    }

    /*
    @Override
    public List<Map> read(Map rule) throws Exception {
        StringBuilder cols = new StringBuilder();

        if( rule !=null && rule.size() > 0 ) {
            int i = 0;
            for (Object sCol : rule.keySet()) {
                if (i > 0) {
                    cols.append(", ");
                }
                cols.append(sCol).append(" AS ").append(rule.get(sCol));

                i++;
            }
        } else {
            cols.append("*");
        }

        String query = "SELECT " + cols + " FROM " + context.evaluate(getOutput());
        System.out.println(query);

        PreparedStatement ps = this.connection.prepareStatement(query);

        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmeta = rs.getMetaData();

        List<Map> result = new ArrayList<>();
        int count = rsmeta.getColumnCount();
        while(rs.next() ) {
            Map<String, Object> column = new LinkedHashMap<>();
            for( int i = 1; i <= count; i++ ) {
                Object row = rs.getObject(i);

                column.put(rsmeta.getColumnLabel(i), row);
            }

            result.add(column);
        }

        ps.close();

        return result;
    }
    */

    @Override
    public int update(MappingRule mappingRule, Object data) {
        return 0;
    }

    @Override
    public int delete() throws Exception {
        String table = (String) this.context.get("SEQUENCE.TARGET");

        String truncateSQL = "DELETE FROM " + table;
        try (PreparedStatement ps = this.connection.prepareStatement(truncateSQL)) {
            int timeout = this.context.getIntegerDefault("SEQUENCE.TIMEOUT", 0);
            ps.setQueryTimeout(timeout);
            ps.execute();
            this.connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return 0;
    }

    @Override
    public void check() {

    }

    @Override
    public void close() throws Exception {
        try {
            if( this.connection != null ) {
                this.connection.close();
            }
        } finally {
            this.connection = null;
        }
    }

    @Override
    public int getSent() {
        return sent;
    }
}
