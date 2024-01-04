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
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Date;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectorSFTP extends ConnectorBase {
    Logger logger = LogManager.getLogger(ConnectorSFTP.class);
    Module module = null;

    Context channelContext = null;
    ModuleContext moduleContext = null;

    private BufferedOutputStream outputStream;

    private Session session = null;
    private Channel channel = null;
    private ChannelSftp sftp = null;

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

        String target = PropertyMap.getString(this.module.getSequences(), FlowTag.TARGET);
        remoteFile = context.evaluate(target);
        String baseDir = remoteFile.substring(0, remoteFile.lastIndexOf("/"));
        remoteFile = remoteFile.substring(remoteFile.lastIndexOf("/") + 1);

        JSch jsch = new JSch();

        try {
            String[] connect = url.split(":");
            session = jsch.getSession(user, connect[0], Integer.parseInt(connect[1]));
            session.setPassword(password);

            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();

            channel = session.openChannel("sftp");
            channel.connect();

            sftp = (ChannelSftp)channel;
            //System.out.println("=> Connected to " + host);
            sftp.cd(baseDir);
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void beforeCreate(MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String header = getDocumentTemplate().makeHeader(context, mappingRule, null);

        writeFile(header, ChannelSftp.OVERWRITE);
    }

    @Override
    public int create(List<Map> itemList, MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String body = getDocumentTemplate().makeBody(context, mappingRule, itemList, sent);

        writeFile(body, ChannelSftp.APPEND);

        this.sent += itemList.size();
        return itemList.size();
    }

    @Override
    public int createPartially(List<Map> items, MappingRule mappingRule) throws Exception {
        ModuleContext context = this.module.getContext();
        String body = getDocumentTemplate().makeBody(context, mappingRule, items, sent);

        writeFile(body, ChannelSftp.APPEND);

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
            writeFile(footer, ChannelSftp.APPEND);
        }
    }

    public int writeFile(String buffer, int mode) throws Exception {
        Map connectInfo = (Map) this.module.getContext().get(MODULE_CONTEXT.CONNECT_INFO);
        String code = PropertyMap.getStringDefault(connectInfo, ConnectorTag.CODE, "UTF-8");

        InputStream input = new ByteArrayInputStream(buffer.getBytes(code));

        try {
            this.sftp.put(input, remoteFile, mode);
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








