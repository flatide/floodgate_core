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
import com.flatide.floodgate.system.FlowEnv;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Date;
import java.util.*;

public class ConnectorFile extends ConnectorBase {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    //Logger logger = LogManager.getLogger(FileConnector.class);

    private BufferedOutputStream outputStream;

    private int sent = 0;

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
    public void connect(Context context) throws Exception {
        super.connect(context);

        String filename = context.evaluate(getOutput());
        this.outputStream = new BufferedOutputStream(new FileOutputStream(new File(this.url + "/" + filename)));
    }

    @Override
    public void beforeCreate(MappingRule mappingRule) throws Exception {
        String header = getDocumentTemplate().makeHeader(context, mappingRule, null);
        this.outputStream.write(header.getBytes());

        super.beforeCreate(mappingRule);
    }

    @Override
    public int creating(List<Map<String, Object>> itemList, MappingRule mappingRule, long index, int batchSize) throws Exception {
        String body = getDocumentTemplate().makeBody(context, mappingRule, itemList, index);
        this.outputStream.write(body.getBytes());

        this.sent += itemList.size();
        return itemList.size();
    }

    @Override
    public int creatingBinary(byte[] item, long size, long sent) throws Exception {
        this.outputStream.write(item, 0, (int) size);
        this.sent += size;
        return (int) size;
    }

    @Override
    public void afterCreate(MappingRule mappingRule) throws Exception {
        String footer = getDocumentTemplate().makeFooter(context, mappingRule, null);
        this.outputStream.write(footer.getBytes());

        this.outputStream.flush();

        super.afterCreate(mappingRule);
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
    public void check() {

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
}
