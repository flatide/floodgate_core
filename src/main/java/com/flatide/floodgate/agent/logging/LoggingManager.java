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

package com.flatide.floodgate.agent.logging;

import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.system.datasource.FDataSource;
import com.flatide.floodgate.system.datasource.FDataSourceDB;
import com.flatide.floodgate.system.datasource.FDataSourceDefault;
import com.flatide.floodgate.system.datasource.FDataSourceFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/*
    MetaManager와 달리 메모리에 캐시하지 않는다
 */

public final class LoggingManager {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    private static final Logger logger = LogManager.getLogger(LoggingManager.class);

    private static final LoggingManager instance = new LoggingManager();

    // data source
    FDataSource dataSource;

    Map<String, String> tableKeyMap;

    private LoggingManager() {
        try {
            setDataSource(new FDataSourceDefault());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static LoggingManager shared() {
        return instance;
    }

    public void setDataSource(FDataSource dataSource) throws Exception {
        setDataSource(dataSource, true);
    }

    public void setDataSource(FDataSource FGDataSource, boolean reset) throws Exception {
        logger.debug("Set DataSource as " + FGDataSource.getName() + " with reset = " + reset);

        this.tableKeyMap = new HashMap<>();
        this.dataSource = FGDataSource;
        FGDataSource.connect();
    }

    public FDataSource changeSource(String source, boolean reset) throws Exception {
        if( !source.equals(this.dataSource.getName()) ) {
            String type = ConfigurationManager.shared().getString("datasource." + source + ".type");
            if (type.equals("FILE")) {
                dataSource = new FDataSourceFile(source);
            } else if (type.equals("DB")) {
                dataSource = new FDataSourceDB(source);
            } else {
                dataSource = new FDataSourceDefault();
            }

            setDataSource(dataSource, reset);
        }

        return this.dataSource;
    }

    public void close() {
        this.tableKeyMap = null;

        if( this.dataSource != null ) {
            this.dataSource.close();
            this.dataSource = null;
        }
    }

    // 메타 생성
    private boolean create(String key) {
        return dataSource.create(key);
    }

    // 메타 수정
    public boolean insert(String tableName, String keyName, Map<String, Object> data) {
        try {
            return dataSource.insert(tableName, keyName, data);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // 메타 수정
    public boolean update(String tableName, String keyName, Map<String, Object> data) {
        try {
            return dataSource.update(tableName, keyName, data);
        } catch(Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean read(String tableName, String keyName, Map<String, Object> data) {
        try {
            return dataSource.update(tableName, keyName, data);
        } catch(Exception e) {
            e.printStackTrace();
        }

        return false;
    }
}
