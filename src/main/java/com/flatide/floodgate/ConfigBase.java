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

package com.flatide.floodgate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ConfigBase {
    private Map<String, Object> config = new HashMap<>();

    public Map<String, Object> getConfig() {
        return this.config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public Object get(String path) {
        String[] keys = path.split("\\.");

        Object cur = this.config;
        for( int i = 0; i < keys.length; i++ ) {
            if( cur == null || cur instanceof String ) {
                return null;
            }

            String key  = keys[i];

            if( cur instanceof String ) {
            } else if( cur instanceof Map) {
                cur = ((Map) cur).get(key);
            } else if( cur instanceof List) {
                Integer num = Integer.parseInt(key);
                cur = ((List) cur).get(num);
            }
        }

        return cur;
    }
}
