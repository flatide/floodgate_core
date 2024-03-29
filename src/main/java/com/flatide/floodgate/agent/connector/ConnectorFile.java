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
import com.flatide.floodgate.agent.flow.rule.MappingRule;
import com.flatide.floodgate.agent.flow.rule.MappingRuleItem;
import com.flatide.floodgate.agent.flow.rule.FunctionProcessor;
import com.flatide.floodgate.agent.flow.FlowTag;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.flow.module.ModuleContext;
import com.flatide.floodgate.agent.flow.module.ModuleContext.MODULE_CONTEXT;
import com.flatide.floodgate.system.FlowEnv;
import com.flatide.floodgate.system.utils.PropertyMap;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Date;
import java.util.*;

public class ConnectorFile extends ConnectorBase {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    //Logger logger = LogManager.getLogger(FileConnector.class);
    Module module = null;

    private BufferedOutputStream outputStream;

    private int sent = 0;
    private int errorPosition = -1;

    private static class FileFunctionProcessor implements FunctionProcessor {
        public Object process(MappingRuleItem item) {
            String func = item.getSourceName();
            MappingRule.Function function = MappingRule.Function.valueOf(func);

            switch( function ) {
                case TARGET_DATE: {
                    long current = System.currentTimeMillis();
                    return new Date(current);
                }
                case DATE: {
                    long current = System.currentTimeMillis();
                    return new Date(current);
                }
                case SEQ:
                    return FlowEnv.shared().getSequence();
                default:
                    return null;
            }
        }
    }
    static FunctionProcessor PRE_PROCESSOR = new FileFunctionProcessor();

    @Override
    public FunctionProcessor getFunctionProcessor(String type) {
        return PRE_PROCESSOR;
    }

    @Override
    public void connect(Context context, Module module) throws Exception {
        this.module = module;

        Map connectInfo = (Map) module.getContext().get(MODULE_CONTEXT.CONNECT_INFO);
        String url = PropertyMap.getString(connectInfo, ConnectorTag.URL);

        String target = PropertyMap.getString(this.module.getSequences(), FlowTag.TARGET);
        String filename = context.evaluate(target);
        this.outputStream = new BufferedOutputStream(new FileOutputStream(new File(url + "/" + filename)));
    }

    @Override
    public void beforeCreate(MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String header = getDocumentTemplate().makeHeader(context, mappingRule, null);
        this.outputStream.write(header.getBytes());
    }

    @Override
    public int create(List<Map> itemList, MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String body = getDocumentTemplate().makeBody(context, mappingRule, itemList, sent);

        this.outputStream.write(body.getBytes());

        this.sent += itemList.size();
        return itemList.size();
    }

    @Override
    public int createPartially(List<Map> items, MappingRule mappingRule) throws Exception {
        return 0;
    }

    public int creatingBinary(byte[] item, long size, long sent) throws Exception {
        this.outputStream.write(item, 0, (int) size);
        this.sent += size;
        return (int) size;
    }

    @Override
    public void afterCreate(MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String footer = getDocumentTemplate().makeFooter(context, mappingRule, null);
        this.outputStream.write(footer.getBytes());

        this.outputStream.flush();
    }

    @Override
    public void beforeRead(MappingRule rule) throws Exception {
    }

    @Override
    public int readBuffer(MappingRule rule, List buffer, int limit) throws Exception {
        return 0;
    }

    @Override
    public List<Map> readPartially(MappingRule rule) throws Exception {
        return null;
    }

    @Override
    public void afterRead() throws Exception {
    }

    @Override
    public List<Map> read(MappingRule rule) {
        return null;
    }

    @Override
    public int update(MappingRule mappingRule, Object data) {
        return 0;
    }

    @Override
    public int delete() {
        return 0;
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public void rollback() throws Exception {
    }

    @Override
    public void close() throws Exception {
        try {
            if (this.outputStream != null) {
                this.outputStream.flush();
                this.outputStream.close();
            }
        } finally {
            this.outputStream = null;
        }
    }

    @Override
    public int getSent() {
        return sent;
    }

    @Override
    public int getErrorPosition() {
        return errorPosition;
    }
}
