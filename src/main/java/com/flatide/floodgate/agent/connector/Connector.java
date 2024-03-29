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
import com.flatide.floodgate.agent.template.DocumentTemplate;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.flow.rule.FunctionProcessor;
import com.flatide.floodgate.agent.flow.rule.MappingRule;

import java.util.List;
import java.util.Map;

public interface Connector {
    DocumentTemplate getDocumentTemplate();
    void setDocumentTemplate(DocumentTemplate template);
    public FunctionProcessor getFunctionProcessor(String type);

    void connect(Context context, Module module) throws Exception;

    void check() throws Exception;
    void count() throws Exception;

    void beforeRead(MappingRule rule) throws Exception;
    List<Map> read(MappingRule rule) throws Exception;
    List<Map> readPartially(MappingRule rule) throws Exception;
    int readBuffer(MappingRule rule, List buffer, int limit) throws Exception;
    void afterRead() throws Exception;

    void beforeCreate(MappingRule rule) throws Exception;
    int create(List<Map> items, MappingRule mappingRule) throws Exception;
    int createPartially(List<Map> items, MappingRule mappingRule) throws Exception;
    void afterCreate(MappingRule rule) throws Exception;

    int update(MappingRule mappingRule, Object data) throws Exception;
    int delete() throws Exception;

    void commit() throws Exception;
    void rollback() throws Exception;

    void close() throws Exception;

    int getSent();
    int getErrorPosition();
}
