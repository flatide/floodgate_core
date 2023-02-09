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

public class FDataSourceDefault implements FDataSource {
    String name;

    public FDataSourceDefault() {
        this.name = "Default";
    }

    public FDataSourceDefault(String name) {
        this.name = name;
    }

    @Override
    public boolean connect() throws Exception {
        return false;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public List<String> getAllKeys(String tableName, String keyColumn) throws Exception {
        return null;
    }

    @Override
    public boolean create(String key) {
        return false;
    }

    @Override
    public Map<String, Object> read(String tableName, String keyColumn, String key) throws Exception {
        return null;
    }

    @Override
    public List<Map<String, Object>> readList(String tableName, String keyColumn, String key) throws Exception {
        return null;
    }

    @Override
    public boolean insert(String tableName, String keyColumn, Map<String, Object> row) throws Exception {
        return false;
    }

    @Override
    public boolean update(String tableName, String keyColumn, Map<String, Object> row) throws Exception {
        return false;
    }

    @Override
    public boolean delete(String tableName, String keyColumn, String key) throws Exception {
        return false;
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

    }
}
