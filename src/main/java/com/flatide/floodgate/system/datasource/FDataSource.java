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

import java.util.List;
import java.util.Map;

public interface FDataSource {
    boolean connect() throws Exception;

    void setName(String name);
    String getName();
    List<String> getAllKeys(String tableName, String keyColumn) throws Exception;

    boolean create(String key);
    Map<String, Object> read(String tableName, String keyColumn, String key) throws Exception;
    List<Map<String, Object>> readList(String tableName, String keyColumn, String key) throws Exception;
    boolean insert(String tableName, String keyColumn, Map<String, Object> row) throws Exception;
    boolean update(String tableName, String keyColumn, Map<String, Object> row) throws Exception;
    boolean delete(String tableName, String keyColumn, String key) throws Exception;

    int deleteAll() throws Exception;
    void flush() throws Exception;
    void close();
}
