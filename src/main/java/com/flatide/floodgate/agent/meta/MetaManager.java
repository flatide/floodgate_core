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

package com.flatide.floodgate.agent.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.system.datasource.FDataSource;
import com.flatide.floodgate.system.datasource.FDataSourceDB;
import com.flatide.floodgate.system.datasource.FDataSourceDefault;
import com.flatide.floodgate.system.datasource.FDataSourceFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.Reader;
import java.sql.Clob;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/*
    TODO Meta의 생성, 삭제, 변경등은 반드시 DataSource에 먼저 반영하고 캐시를 업데이트 하는 순서로 진행할 것!
    캐시 업데이트 타이밍은 실시간일 필요는 없다.
 */
public final class MetaManager {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    private static final Logger logger = LogManager.getLogger(MetaManager.class);

    private static final MetaManager instance = new MetaManager();

    // cache - permanent 구조
    // TODO datasource에서 caching하도록 수정할 것
    Map<String, MetaTable> cache;
    Map<String, String> tableKeyMap;

    // data source
    FDataSource dataSource;

    public static MetaManager shared() {
        return instance;
    }

    private MetaManager() {
        try {
            setDataSource(new FDataSourceDefault());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public FDataSource changeSource(String source, boolean reset) throws Exception {
        if( !source.equals(getMetaSourceName()) ) {
            String type = ConfigurationManager.shared().getString("datasource."+ source + ".type");
            if (type.equals("FILE")) {
                dataSource = new FDataSourceFile(source);
            } else if (type.equals("DB")) {
                dataSource = new FDataSourceDB(source);
            } else {
                dataSource = new FDataSourceDefault();
            }

            setMetaSource(dataSource, reset);
        }

        return this.dataSource;
    }


    public void close() {
        if( this.cache != null ) {
            // TODO store cache to permanent storage
            this.cache = null;
            this.tableKeyMap = null;
        }

        if( this.dataSource != null ) {
            this.dataSource.close();
            this.dataSource = null;
        }
    }

    public String getMetaSourceName() {
        return this.dataSource.getName();
    }

    public Map<String, Object> getInfo() {
        Map<String, Object> info = new HashMap<>();

        info.put("MetaSource", this.dataSource.getName());

        for( Map.Entry<String, MetaTable> entry : this.cache.entrySet() ) {
            info.put(entry.getKey(), (entry.getValue()).size());
        }

        return info;
    }

    public void setDataSource(FDataSource dataSource) throws Exception {
        setMetaSource(dataSource, true);
    }

    public void setMetaSource(FDataSource FGDataSource, boolean reset) throws Exception {
        logger.debug("Set MetaSource as " + FGDataSource.getName() + " with reset = " + reset);
        if( reset ) {
            if (this.cache != null) {
                // TODO store cache to permanent storage
                this.cache = null;
            }

            this.cache = new HashMap<>();
        }
        this.tableKeyMap = new HashMap<>();
        this.dataSource = FGDataSource;
        FGDataSource.connect();
    }

    public MetaTable getTable(String tableName ) {
        return this.cache.get(tableName);
    }

    /*public void addKeyName(String table, String key) {
        this.tableKeyMap.put(table, key);
    }*/

    public boolean create(String table, String key) {
        return create(key);
    }

    // 메타 생성
    private boolean create(String key) {
        if (this.cache.get(key) != null) {
            return false;
        }

        if (dataSource.create(key)) {
            this.cache.put(key, null);
            return true;
        }

        return false;
    }

    // 메타 조회
    public Map<String, Object> read(String tableName, String key ) throws Exception {
        return read(tableName, key, false);
    }

    // 메타 조회
    public Map<String, Object> read(String tableName, String key, boolean fromSource ) throws Exception {
        // TODO for testing
        fromSource = true;

        MetaTable table = this.cache.get(tableName);
        if( table == null ) {
            table = new MetaTable();
            this.cache.put(tableName, table);
        }

        String keyName = this.tableKeyMap.get(tableName);
        if( keyName == null ) {
            keyName = "ID";
        }

        if( /*output.get(key) == null || */fromSource) {
            try {
                Map<String, Object> result = this.dataSource.read(tableName, keyName, key);

                for(Entry<String, Object> e : result.entrySet()) {
                    Object obj = e.getValue();

                    if( obj instanceof oracle.sql.TIMESTAMP) {
                        // Jackson cannot (de)serialize oracle.sql.TIMESTAMP, converting it to java.sql.Timestamp
                        obj = ((oracle.sql.TIMESTAMP)obj).timestampValue();
                        result.put(e.getKey(), obj);
                    } else if (obj instanceof Clob) {
                        // Jackson cannot convert LOB directly, converting it to String
                        final StringBuilder sb = new StringBuilder();
                        final Reader reader = ((Clob) obj).getCharacterStream();
                        final BufferedReader br = new BufferedReader(reader);

                        int b;
                        while (-1 != (b = br.read())) {
                            sb.append((char) b);
                        }

                        br.close();

                        ObjectMapper mapper = new ObjectMapper();
                        Map json = mapper.readValue(sb.toString(), Map.class);
                        result.put(e.getKey(), json);
                    }
                }

                return result;
            } catch(Exception e ) {
                e.printStackTrace();
                throw e;
            }
        }

        return null;
    }

    public List<Map<String, Object>> readList(String tableName, String key) throws Exception {
        return readList(tableName, key, false);
    }

    public List<Map<String, Object>> readList(String tableName, String key, boolean fromSource ) throws Exception {
        fromSource = true;

        List<Map<String, Object>> resultList = null;
        MetaTable table = this.cache.get(tableName);
        if( table == null ) {
            table = new MetaTable();
            this.cache.put(tableName, table);
        }

        String keyName = this.tableKeyMap.get(tableName);
        if( keyName == null ) {
            keyName = "ID";
        }

        if( fromSource ) {
            try {
                resultList = this.dataSource.readList(tableName, keyName, key);

                for(Map<String, Object> result : resultList ) {
                    for(Entry<String, Object> e : result.entrySet()) {
                        Object obj = e.getValue();

                        if( obj instanceof oracle.sql.TIMESTAMP) {
                            // Jackson cannot (de)serialize oracle.sql.TIMESTAMP, converting it to java.sql.Timestamp
                            obj = ((oracle.sql.TIMESTAMP)obj).timestampValue();
                            result.put(e.getKey(), obj);
                        } else if (obj instanceof Clob) {
                            // Jackson cannot convert LOB directly, converting it to String
                            final StringBuilder sb = new StringBuilder();
                            final Reader reader = ((Clob) obj).getCharacterStream();
                            final BufferedReader br = new BufferedReader(reader);

                            int b;
                            while (-1 != (b = br.read())) {
                                sb.append((char) b);
                            }

                            br.close();

                            ObjectMapper mapper = new ObjectMapper();
                            Map<String, Object> json = (Map<String, Object>) mapper.readValue(sb.toString(), Map.class);
                            result.put(e.getKey(), json);
                        }
                    }
                }

                //table.put(key, result);
                return resultList;
            } catch(Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        return null;
    }

    // 메타 수정
    public boolean insert(String tableName, String keyName, Map<String, Object> data, boolean toSource) throws Exception {
        // TODO Thread Safe
        MetaTable table = this.cache.get(tableName);

        if (table == null) {
            table = new MetaTable();
            this.cache.put(tableName, table);
        }

        /*String keyName = this.tableKeyMap.get(tableName);
        if( keyName == null ) {
            keyName = "ID";
        }*/

        try {
            boolean result = true;
            if( toSource ) {
                result = dataSource.insert(tableName, keyName, data);
            }
            if( result ) {
                table.put(keyName, data);
            }

            return result;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public boolean update(String tableName, String keyName, Map<String, Object> data) throws Exception {
        return update(tableName, keyName, data, false);
    }

    // 메타 수정
    public boolean update(String tableName, String keyName, Map<String, Object> data, boolean toSource) throws Exception {
        // TODO Thread Safe
        MetaTable table = this.cache.get(tableName);

        if (table == null) {
            return false;
        }

        /*String keyName = this.tableKeyMap.get(tableName);
        if( keyName == null ) {
            keyName = "ID";
        }*/

        try {
            boolean result = true;
            if( toSource ) {
                result = dataSource.update(tableName, keyName, data);
            }
            if( result ) {
                table.put(keyName, data);
            }

            return result;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    // 메타 삭제
    public boolean delete(String tableName, String key, boolean toSource) throws Exception {
        MetaTable table = this.cache.get(tableName);

        if (table == null) {
            return false;
        }

        String keyName = this.tableKeyMap.get(tableName);
        if( keyName == null ) {
            keyName = "ID";
        }

        try {
            boolean result = true;
            if (toSource) {
                result = dataSource.delete(tableName, keyName, key);
            }
            if (result) {
                table.remove(key);
            }
            return true;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public boolean load(String tableName) throws Exception {
        MetaTable table = this.cache.get(tableName);
        if( table == null ) {
            table = new MetaTable();//HashMap<>();
            this.cache.put(tableName, table);
        }

        String keyName = this.tableKeyMap.get(tableName);
        if( keyName == null ) {
            keyName = "ID";
        }


        List<String> keyList = this.dataSource.getAllKeys(tableName, keyName);

        for( String key : keyList ) {
            read(tableName, key, true);
        }

        return true;
    }

    // 메타 저장 cache -> metaSource
    public boolean store(String tableName, boolean all) throws Exception {
        MetaTable table = this.cache.get(tableName);

        if( table == null ) {
            return false;
        }

        String keyName = this.tableKeyMap.get(tableName);
        if( keyName == null ) {
            keyName = "ID";
        }

        Map<String, Map<String, Object>> rows = table.getRows();

        for( Map.Entry<String, Map<String, Object>> e : rows.entrySet() ) {
            String key = e.getKey();
            Map<String, Object> data = e.getValue();
            if( !dataSource.update(tableName, keyName, data) ) {
                dataSource.insert(tableName, keyName, data);
            }
        }

        return true;
    }
}
