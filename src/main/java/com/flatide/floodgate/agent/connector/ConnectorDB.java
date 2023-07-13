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
import com.flatide.floodgate.agent.Context.CONTEXT_KEY;
import com.flatide.floodgate.agent.flow.rule.MappingRuleItem;
import com.flatide.floodgate.agent.handler.HandlerManager;
import com.flatide.floodgate.agent.handler.HandlerManager.Step;
import com.flatide.floodgate.agent.template.DocumentTemplate;
import com.flatide.floodgate.agent.flow.rule.MappingRule;
import com.flatide.floodgate.agent.flow.FlowTag;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.flow.module.ModuleContext;
import com.flatide.floodgate.agent.flow.module.ModuleContext.MODULE_CONTEXT;
import com.flatide.floodgate.agent.flow.rule.FunctionProcessor;
import com.flatide.floodgate.system.FlowEnv;
import com.flatide.floodgate.system.security.FloodgateSecurity;
import com.flatide.floodgate.system.utils.PropertyMap;

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

    Context channelContext = null;
    ModuleContext moduleContext = null;

    Module module = null;

    private Connection connection = null;
    private Integer fetchSize = 0;
    private Integer sizeForUpdateHandler = 0;
    private Boolean flush = false;
    private int retrieve = 0;

    private Integer batchSize = 0;
    private String query = "";
    private PreparedStatement ps = null;
    private ResultSet resultSet = null;

    private Integer batchCount = 0;

    private List<String> param;

    private int sent = 0;

    private int updateCount = 0;

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

        this.module = module;

        Map connectInfo = (Map) module.getContext().get(MODULE_CONTEXT.CONNECT_INFO);
        String url = PropertyMap.getString(connectInfo, ConnectorTag.URL);
        String user = PropertyMap.getString(connectInfo, ConnectorTag.USER);
        String password = PropertyMap.getString(connectInfo, ConnectorTag.PASSWORD);

        channelContext = (Context) this.module.getFlowContext().get(CONTEXT_KEY.CHANNEL_CONTEXT);
        moduleContext = module.getContext();

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
            password = FloodgateSecurity.shared().decrypt(password);
            this.connection = DriverManager.getConnection(url, user, password);
        }

        this.connection.setAutoCommit(false);

        DatabaseMetaData meta = this.connection.getMetaData();
        logger.debug(meta.getDatabaseProductName() + " : " + meta.getDatabaseProductVersion());
    }

    @Override
    public void beforeCreate(MappingRule mappingRule) throws Exception {
        this.batchSize = PropertyMap.getIntegerDefault(module.getSequences(), FlowTag.BATCHSIZE, 1);
    }

    @Override
    public int createPartially(List<Map> itemList, MappingRule mappingRule) throws Exception {
        if( this.query.isEmpty()) {
            if (itemList == null || itemList.isEmpty()) {
                return 0;
            }
            DocumentTemplate documentTemplate = getDocumentTemplate();

            List<Map<String, Object>> temp = new ArrayList<>();
            Map<String, Object> one = itemList.get(0);
            Map<String, Object> copy = new HashMap<>();
            for( Map.Entry<String, Object> e : one.entrySet() ) {
                copy.put(e.getKey(), "?");
            }
            temp.add(copy);

            this.query = documentTemplate.makeHeader(moduleContext, mappingRule, temp);
            this.param = mappingRule.getParam();
            logger.debug(this.query);
            ps = this.connection.prepareStatement(this.query);
        }

        try {
            int timeout = PropertyMap.getIntegerDefault(module.getSequences(), FlowTag.TIMEOUT, 0);
            if (itemList != null) {
                for (Map item : itemList) {
                    int i = 1;
                    for (String key : this.param) {
                        if (key.startsWith(">")) {
                            Object value = processEmbedFunction(key.substring(1));
                            ps.setObject(i++, value);
                        } else if (key.startsWith("{")) {
                            Object value = moduleContext.get(key.substring(1, key.length() - 1));
                            ps.setObject(i++, value);
                        } else {
                            ps.setObject(i++, item.get(key));
                        }
                    }

                    ps.addBatch();
                    batchCount++;
                    if (batchCount >= batchSize) {
                        ps.setQueryTimeout(timeout);
                        cur = System.currentTimeMillis();
                        ps.executeBatch();
                        this.sent += batchCount;
                        batchCount = 0;

                        this.module.setProgress(this.sent);
                        HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
                    }
                }
                itemList.clear();
            }
            if (itemList == null && batchCount > 0) {
                ps.setQueryTimeout(timeout);
                ps.executeBatch();
                this.sent += batchCount;
                this.module.setProgress(this.sent);
                HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        return sent;
    }

    @Override
    public int create(List<Map> itemList, MappingRule mappingRule) throws Exception {
        if (this.query.isEmpty()) {
            DocumentTemplate documentTemplate = getDocumentTemplate();

            List<Map<String, Object>> temp = new ArrayList<>();
            Map<String, Object> one = itemList.get(0);
            Map<String, Object> copy = new HashMap<>();
            for (Map.Entry<String, Object> e : one.entrySet()) {
                copy.put(e.getKey(), "?");
            }
            temp.add(copy);

            this.query = documentTemplate.makeHeader(moduleContext, mappingRule, temp);
            this.param = mappingRule.getParam();
            logger.debug(this.query);
            ps = this.connection.prepareStatement(this.query);
        }

        try {
            int count = 0;
            int timeout = PropertyMap.getIntegerDefault(module.getSequences(), FlowTag.TIMEOUT, 0);
            for (Map item : itemList) {
                int i = 1;
                for (String key : this.param) {
                    if (key.startsWith(">")) {
                        Object value = processEmbedFunction(key.substring(1));
                        ps.setObject(i++, value);
                    } else if (key.startsWith("{")) {
                        Object value = moduleContext.get(key.substring(1, key.length() - 1));
                        ps.setObject(i++, value);
                    } else {
                        ps.setObject(i++, item.get(key));
                    }
                }

                ps.addBatch();
                count++;
                if (count >= batchSize) {
                    ps.setQueryTimeout(timeout);
                    ps.executeBatch();
                    this.sent += count;
                    count = 0;

                    this.module.setProgress(this.sent);
                    HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
                }
            }
            if (count > 0) {
                ps.setQueryTimeout(timeout);
                ps.executeBatch();
                this.sent += count;
                this.module.setProgress(this.sent);
                HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        return sent;
    }

    @Override
    public void afterCreate(MappingRule rule) throws Exception {
    }

    @Override
    public void commit() throws SQLException {
        this.connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        this.sent = 0;
        this.connection.rollback();
        this.module.setProgress(0);
        HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
    }

    @Override
    public void beforeRead(MappingRule rule) throws Exception {
        String table = PropertyMap.getString(this.module.getSequences(), FlowTag.TARGET);
        String sql = PropertyMap.getString(this.module.getSequences(), FlowTag.SQL);
        String condition = PropertyMap.getString(this.module.getSequences(), FlowTag.CONDITION);

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
                if (i > 0) {
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

        this.fetchSize = PropertyMap.getIntegerDefault(this.module.getSequences(), FlowTag.FETCHSIZE, 0);
        this.sizeForUpdateHandler = fetchSize < 1000 ? 3000 : fetchSize * 3;
        this.flush = (Boolean) PropertyMap.getDefault(this.module.getSequences(), FlowTag.FLUSH, Boolean.valueOf(false));

        this.ps = this.connection.prepareStatement(query);
        this.resultSet = ps.executeQuery();

        if (fetchSize > 0) {
            this.resultSet.setFetchSize(fetchSize);
        }
    }

    @Override
    public int readBuffer(MappingRule rule, List buffer, int limit) throws Exception {
        ResultSetMetaData rsmeta = this.resultSet.getMetaData();

        int count = rsmeta.getColumnCount();
        int c = 0;
        while (this.resultSet.next()) {
            if (!this.flush) {
                Map<String, Object> column = new LinkedHashMap<>();
                for (int i = 1; i <= count; i++) {
                    Object row = this.resultSet.getObject(i);

                    if (row instanceof oracle.sql.TIMESTAMP) {
                        // Jackson cannot (de)serialize oracle.sql.TIMESTAMP, converting it to java.sql.Timestamp
                        row = ((oracle.sql.TIMESTAMP)row).timestampValue();
                    }
                    // TODO process Clob and skip Blob
                    column.put(rsmeta.getColumnLabel(i), row);
                }

                buffer.add(column);
            }

            c++;
            if (c >= limit) {
                this.retrieve += c;
                c = 0;
                break;
            }
            if (this.updateCount >= this.sizeForUpdateHandler) {
                this.updateCount = 0;
                this.module.setProgress(retrieve);
                HandlerManager.shared().handle(Step.MODULE_PROGRESS, this.channelContext, this.module);
                break;
            }
        }
        if ( c > 0 ) {
            this.retrieve += c;
            this.module.setProgress(retrieve);
            HandlerManager.shared().handle(Step.MODULE_PROGRESS, this.channelContext, this.module);
        }

        return buffer.size();
    }

    @Override
    public List<Map> readPartially(MappingRule rule) throws Exception {
        List<Map> result = new ArrayList<>();

        ResultSetMetaData rsmeta = this.resultSet.getMetaData();


        int count = rsmeta.getColumnCount();
        int c = 0;
        while (this.resultSet.next()) {
            if (!this.flush) {
                Map<String, Object> column = new LinkedHashMap<>();
                for (int i = 1; i <= count; i++) {
                    Object row = this.resultSet.getObject(i);

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
            if (c >= this.sizeForUpdateHandler) {
                this.retrieve += c;
                c = 0;
                this.module.setProgress(retrieve);
                HandlerManager.shared().handle(Step.MODULE_PROGRESS, this.channelContext, this.module);
                break;
            }
        }
        if ( c > 0 ) {
            this.retrieve += c;
            this.module.setProgress(retrieve);
            HandlerManager.shared().handle(Step.MODULE_PROGRESS, this.channelContext, this.module);
        }

        return result;
    }

    @Override
    public void afterRead() throws Exception {
    }

    @Override
    public List<Map> read(MappingRule rule) throws Exception {
        List<Map> result = new ArrayList<>();
        ResultSetMetaData rsmeta = this.resultSet.getMetaData();

        int count = rsmeta.getColumnCount();
        int c = 0;
        while (this.resultSet.next()) {
            if (!this.flush) {
                Map<String, Object> column = new LinkedHashMap<>();
                for (int i = 1; i <= count; i++) {
                    Object row = this.resultSet.getObject(i);

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
        if ( c > 0 ) {
            retrieve += c;
            this.module.setProgress(retrieve);
            HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
        }

        return result;
    }

    @Override
    public void check() throws Exception {
        String table = PropertyMap.getString(this.module.getSequences(), FlowTag.TARGET);

        DatabaseMetaData databaseMetaData = this.connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getTables(null, null, table, new String[] {"TABLE"});
        boolean exist = resultSet.next();

        if (!exist) {
            throw new Exception(table + " is not exist.");
        }
    }

    @Override
    public void count() throws Exception {
        String table = PropertyMap.getString(this.module.getSequences(), FlowTag.TARGET);
        String sql = PropertyMap.getString(this.module.getSequences(), FlowTag.SQL);
        String condition = PropertyMap.getString(this.module.getSequences(), FlowTag.CONDITION);

        String query = "";

        if (sql != null) {
            query = sql;
        } else {
            query = "SELECT COUNT(*) AS COUNT ";
            query += " FROM " + table;
            if (condition != null && !condition.isEmpty()) {
                query += " WHERE " + condition;
            }
            logger.debug(query);
        }

        try (PreparedStatement ps = this.connection.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int count = rs.getInt("COUNT");
                this.module.setProgress(count);
                HandlerManager.shared().handle(Step.MODULE_PROGRESS, channelContext, this.module);
            }
        } catch (Exception e) {
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
        String table = PropertyMap.getString(this.module.getSequences(), FlowTag.TARGET); 

        String truncateSQL = "DELETE FROM " + table;
        try (PreparedStatement ps = this.connection.prepareStatement(truncateSQL)) {
            int timeout = PropertyMap.getIntegerDefault(this.module.getSequences(), FlowTag.TIMEOUT, 0);
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
    public void close() throws Exception {
        try { if (this.connection != null) this.connection.close(); } finally { this.connection = null; }
        try { if (this.ps != null) this.ps.close(); } finally { this.ps = null; }
        try { if (this.resultSet != null) this.resultSet.close(); } finally { this.resultSet = null; }
    }

    @Override
    public int getSent() {
        return sent;
    }
}
