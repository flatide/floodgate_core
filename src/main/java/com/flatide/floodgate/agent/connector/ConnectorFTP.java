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
import com.flatide.floodgate.agent.Context.CONTEXT_KEY;
import com.flatide.floodgate.agent.flow.rule.MappingRule;
import com.flatide.floodgate.agent.flow.rule.MappingRuleItem;
import com.flatide.floodgate.agent.flow.rule.FunctionProcessor;
import com.flatide.floodgate.agent.flow.FlowTag;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.flow.module.ModuleContext;
import com.flatide.floodgate.agent.flow.module.ModuleContext.MODULE_CONTEXT;
import com.flatide.floodgate.system.FlowEnv;
import com.flatide.floodgate.system.security.FloodgateSecurity;
import com.flatide.floodgate.system.utils.PropertyMap;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Date;
import java.util.*;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectorFTP extends ConnectorBase {
    Logger logger = LogManager.getLogger(ConnectorFTP.class);
    Module module = null;

    Context channelContext = null;
    ModuleContext moduleContext = null;

    private BufferedOutputStream outputStream;

    private FTPClient ftp = null;
    private boolean isLogin = false;

    private int sent = 0;
    private int errorPosition = -1;

    private String remoteFile = "";

    private static class FTPFunctionProcessor implements FunctionProcessor {
        public Object process(MappingRuleItem item) {
            String func = item.getSourceName();
            MappingRule.Function function = MappingRule.Function.valueOf(func);

            switch (function) {
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
    static FunctionProcessor PRE_PROCESSOR = new FTPFunctionProcessor();

    @Override
    public FunctionProcessor getFunctionProcessor(String type) {
        return PRE_PROCESSOR;
    }

    @Override
    public void connect(Context context, Module module) throws Exception {
        this.module = module;

        channelContext = (Context) this.module.getFlowContext().get(CONTEXT_KEY.CHANNEL_CONTEXT);
        moduleContext = module.getContext();

        Map connectInfo = (Map) module.getContext().get(MODULE_CONTEXT.CONNECT_INFO);
        String url = PropertyMap.getString(connectInfo, ConnectorTag.URL);
        String user = PropertyMap.getString(connectInfo, ConnectorTag.USER);
        String password = PropertyMap.getString(connectInfo, ConnectorTag.PASSWORD);
        password = FloodgateSecurity.shared().decrypt(password);

        String target = (String) module.getSequences().get(FlowTag.TARGET);
        remoteFile = context.evaluate(target);

        this.ftp = new FTPClient();

        int reply;
        String[] connect = url.split(":");

        int timeout = PropertyMap.getIntegerDefault(connectInfo, ConnectorTag.TIMEOUT, 0);

        this.ftp.setConnectTimeout(timeout);

        try {
            if (connect.length > 1) {
                this.ftp.connect(connect[0], Integer.parseInt(connect[1]));
            } else {
                this.ftp.connect(connect[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        reply = this.ftp.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            this.ftp.disconnect();
            throw new Exception("Error while connecting FTP server" + reply);
        }

        boolean rt = true;
        rt = this.ftp.login(user, password);
        if (rt == false) {
            throw new Exception("Error while login FTP server");
        }
        this.isLogin = true;
        rt = this.ftp.setFileType(FTP.BINARY_FILE_TYPE);
        if (rt == false) {
            throw new Exception("Error while setting File Type on FTP server");
        }
        rt = this.ftp.changeWorkingDirectory(target);
        if ( rt==false) {
            throw new Exception("Error while changing working Directory on FTP server");
        }

        if (PropertyMap.getStringDefault(connectInfo, ConnectorTag.PASSIVE, "FALSE").equals("TRUE")) {
            this.ftp.enterLocalPassiveMode();
        }

        this.ftp.setRemoteVerificationEnabled(false);
    }

    @Override
    public void beforeCreate(MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String header = getDocumentTemplate().makeHeader(context, mappingRule, null);

        if (!header.isEmpty()) {
            appendFile(header);
        }
    }

    @Override
    public int create(List<Map> itemList, MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String body = getDocumentTemplate().makeBody(context, mappingRule, itemList, sent);

        appendFile(body);

        this.sent += itemList.size();
        return itemList.size();
    }

    @Override
    public int createPartially(List<Map> items, MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String body = getDocumentTemplate().makeBody(context, mappingRule, items, sent);

        appendFile(body);

        this.sent += items.size();
        return items.size();
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
        if (!footer.isEmpty()) {
            appendFile(footer);
        }
    }

    public int appendFile(String buffer) throws Exception {
        Map connectInfo = (Map) this.module.getContext().get(MODULE_CONTEXT.CONNECT_INFO);
        String code = PropertyMap.getStringDefault(connectInfo, ConnectorTag.CODE, "UTF-8");

        InputStream input = new ByteArrayInputStream(buffer.getBytes(code));

        try {
            boolean success = this.ftp.appendFile(remoteFile, input);
            if (!success) {
                logger.info("Append to the " + remoteFile + " is failed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            input.close();
        }

        return 0;
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








