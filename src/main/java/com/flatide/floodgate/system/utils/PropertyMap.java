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

package com.flatide.floodgate.system.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.flatide.floodgate.agent.Context;

public class PropertyMap {
    static public Object get(Map map, Enum key) {
        return get(map, key.name());
    }

    static public Object getDefault(Map map, Enum key, Object defaultValue) {
        return getDefault(map, key.name(), defaultValue);
    }

    static public Object getDefault(Map map, String key, Object defaultValue) {
        Object obj = get(map, key);
        if (obj == null) {
            obj = defaultValue;
        }
        return obj;
    }

    static public Object get(Map map, String key) {
        Object value = map.get(key);
        if (value == null || key.contains(".")) {
            String[] keys = key.split("\\.");
            Map<String, Object> current = map;
            for (String k : keys) {
                Object child = current.get(k);
                if (child != null) {
                    if (key.contains(".")) {
                        key = key.substring(key.indexOf(".") + 1);
                    } else {
                        return child;
                    }
                    if (child instanceof Context) {
                        return ((Context) child).get(key);
                    } else if (child instanceof Map) {
                        current = (Map<String, Object>) child;
                    }
                } else {
                    return null;
                }
            }
        }
        return value;
    }

    static public String getString(Map map, Enum key) {
        return getString(map, key.name());
    }

    static public String getString(Map map, String key) {
        Object value = get(map, key);
        if (value != null) {
            if( value instanceof String) {
                return (String) value;
            } else {
                return value.toString();
            }
        }
        return null;
    }

    static public String getStringDefault(Map map, Enum key, String defaultValue) {
        return getStringDefault(map, key.name(), defaultValue);
    }

    static public String getStringDefault(Map map, String key, String defaultValue) {
        String ret = getString(map, key);
        if( ret == null ) {
            ret = defaultValue;
        }
        return ret;
    }

    static public Integer getInteger(Map map, Enum key) {
        return getInteger(map, key.name());
    }

    static public Integer getInteger(Map map, String key) {
        Object value = get(map, key);
        if( value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return Integer.valueOf((String) value);
        }

        return null;
    }

    static public Integer getIntegerDefault(Map map, Enum key, Integer defaultValue) {
        return getIntegerDefault(map, key.name(), defaultValue);
    }

    static public Integer getIntegerDefault(Map map, String key, Integer defaultValue) {
        Integer ret = getInteger(map, key);
        if( ret == null ) {
            ret = defaultValue;
        }

        return ret;
    }

    /*
        {SEQUENCE.OUTPUT} 형태를 처리한다.
        {SEQUENCE.{INOUT}} 등의 중첩은 처리하지 못한다
     */
    static public String evaluate(Map map, String str) {
        Pattern pattern = Pattern.compile("\\{[^\\s{}]+\\}");
        Matcher matcher = pattern.matcher(str);

        List<String> findGroup = new ArrayList<>();
        while (matcher.find()) {
            findGroup.add(str.substring(matcher.start(), matcher.end()));
        }

        for( String find : findGroup) {
            String value = getString(map, find.substring(1, find.length() - 1));
            if( value != null ) {
                str = str.replace(find, value);
            } else {
            }
        }

        return str;
    }
}
