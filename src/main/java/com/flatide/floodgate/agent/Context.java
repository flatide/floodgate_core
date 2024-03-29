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

package com.flatide.floodgate.agent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Context {
    private static final Logger logger = LogManager.getLogger(Context.class);

    public enum CONTEXT_KEY {
        API,
        CHANNEL_ID,
        
        REQUEST_PATH_VARIABLES,
        REQUEST_PARAMS,
        REQUEST_BODY,
        HTTP_REQUEST_METHOD,

        API_META,
        FLOW_META,

        CHANNEL_CONTEXT,
        FLOW_CONTEXT,

        CHANNEL_START_TIME,
        CHANNEL_ELAPSED,
        FLOW_START_TIME,
        MODULE_START_TIME,

        LATEST_RESULT,
        LATEST_MSG,
    }

    Map<String, Object> map = new HashMap<>();

    public Map<String, Object> getMap() {
        return this.map;
    }

    public void setMap(final Map<String, Object> map) {
        this.map = map;
    }

    public String toString() {
        return this.map.toString();
    }

    public void add(String key, Object value) {
        this.map.put(key, value);
    }

    public void add(Enum key, Object value) {
        add(key.name(), value);
    }

    public Object get(Enum key) {
        return get(key.name());
    }

    public Object getDefault(Enum key, Object defaultValue) {
        return getDefault(key.name(), defaultValue);
    }

    public Object getDefault(String key, Object defaultValue) {
        Object obj = get(key);
        if (obj == null) {
            obj = defaultValue;
        }
        return obj;
    }

    public Object get(String key) {
        // . 구분자를 포함한 상태의 키가 있는지 먼저 검사
        Object value = this.map.get(key);
        if( value == null || key.contains(".")) {
            // . 구분자로 구성된 경우 자식 context 검색
            String[] keys = key.split("\\.");
            Map<String, Object> current = this.map;
            for(String k : keys) {
                Object child = current.get(k);
                if( child != null ) {
                    if( key.contains(".")) {
                        key = key.substring(key.indexOf(".") + 1);
                    } else {
                        return child;
                    }
                    if( child instanceof Context) {
                        return ((Context) child).get(key);
                    } else if( child instanceof Map) {
                        current = (Map<String, Object>) child;
                    }
                } else {
                    return null;
                }
            }
        }
        return value;
    }

    public String getString(Enum key) {
        return getString(key.name());
    }

    public String getString(String key) {
        Object value = get(key);
        if( value != null ) {
            if( value instanceof String) {
                return (String) value;
            } else {
                return value.toString();
            }
        }

        return null;
    }

    public String getStringDefault(Enum key, String defaultValue) {
        return getStringDefault(key.name(), defaultValue);
    }

    public String getStringDefault(String key, String defaultValue) {
        String ret = getString(key);
        if( ret == null ) {
            ret = defaultValue;
        }
        return ret;
    }

    public Integer getInteger(Enum key) {
        return getInteger(key.name());
    }

    public Integer getInteger(String key) {
        Object value = get(key);
        if( value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return Integer.valueOf((String) value);
        }

        return null;
    }

    public Integer getIntegerDefault(Enum key, Integer defaultValue) {
        return getIntegerDefault(key.name(), defaultValue);
    }

    public Integer getIntegerDefault(String key, Integer defaultValue) {
        Integer ret = getInteger(key);
        if( ret == null ) {
            ret = defaultValue;
        }

        return ret;
    }

    /*
        {SEQUENCE.OUTPUT} 형태를 처리한다.
        {SEQUENCE.{INOUT}} 등의 중첩은 처리하지 못한다
     */
    public String evaluate(String str) {
        Pattern pattern = Pattern.compile("\\{[^\\s{}]+\\}");
        Matcher matcher = pattern.matcher(str);

        List<String> findGroup = new ArrayList<>();
        while (matcher.find()) {
            findGroup.add(str.substring(matcher.start(), matcher.end()));
        }

        for( String find : findGroup) {
            String value = getString(find.substring(1, find.length() - 1));
            if( value != null ) {
                str = str.replace(find, value);
            } else {
                logger.error(find + " is wrong expression.");
            }
        }

        return str;
    }
}
