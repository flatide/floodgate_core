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

package com.flatide.floodgate.agent.flow.module;

import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;
import com.flatide.floodgate.agent.template.DocumentTemplate;
import com.flatide.floodgate.system.utils.PropertyMap;
import com.flatide.floodgate.agent.connector.ConnectorTag;
import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.Context.CONTEXT_KEY;
import com.flatide.floodgate.agent.connector.Connector;
import com.flatide.floodgate.agent.flow.Flow;
import com.flatide.floodgate.agent.flow.FlowContext;
import com.flatide.floodgate.agent.flow.FlowMockup;
import com.flatide.floodgate.agent.flow.FlowTag;
import com.flatide.floodgate.agent.flow.module.ModuleContext.MODULE_CONTEXT;
import com.flatide.floodgate.agent.connector.ConnectorFactory;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.agent.flow.stream.FGSharableInputStream;
import com.flatide.floodgate.agent.flow.stream.Payload;
import com.flatide.floodgate.agent.flow.stream.carrier.Carrier;
import com.flatide.floodgate.agent.flow.stream.carrier.container.JSONContainer;
import com.flatide.floodgate.agent.handler.FloodgateHandlerManager;
import com.flatide.floodgate.agent.handler.FloodgateHandlerManager.Step;
import com.flatide.floodgate.agent.flow.rule.MappingRule;
import com.flatide.floodgate.agent.meta.MetaManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Module {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    private static final Logger logger = LogManager.getLogger(Module.class);

    private FlowContext flowContext;
    private ModuleContext context;

    private final String id;
    private final String name;
    private final Map<String, Object> sequences;
    private final Flow flow;

    private Connector connector = null;
    private Map connInfo = null;

    private Integer progress = 0;

    private String result;
    private String msg = "";

    private List<Map> resultList;

    public Module(Flow flow, String name, Map<String, Object> sequences) {
        this.flow = flow;
        this.name = name;
        this.sequences = sequences;

        UUID id = UUID.randomUUID();
        this.id = id.toString();

        context = new ModuleContext();

        flowContext = flow.getContext();
        context.add(CONTEXT_KEY.CHANNEL_CONTEXT, flow.getChannelContext());
    }

    public FlowContext getFlowContext() {
        return flowContext;
    }

    public ModuleContext getContext() {
        return context;
    }
    
    public String getName() {
        return name;
    }

    public Map<String, Object> getSequences() {
        return sequences;
    }

    public String getId() {
        return id;
    }

    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    public Integer getProgress() {
        return progress;
    }

    public String getResult() {
        return result;
    }
    
    public void setResult(String result) {
        this.result = result;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Flow getFlow() {
        return flow;
    }

    /*
        FlowContext의 input에 대한 처리
    */

    public void processBefore(Flow flow, FlowContext flowContext) throws Exception {
        if (!(flow instanceof FlowMockup)) {
            FloodgateHandlerManager.shared().handle(Step.MODULE_IN, flow.getChannelContext(), this);
        }
        try {
            if (this.sequences != null) {
                Object connectRef = this.sequences.get(FlowTag.CONNECT.name());
                if (connectRef == null) {
                    logger.info(flowContext.getId() + " : No connect info for module " + this.name);
                } else {
                    if (connectRef instanceof String) {
                        String table = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_DATASOURCE);
                        Map connMeta = MetaManager.shared().read(table, (String) connectRef);
                        connInfo = (Map) connMeta.get("DATA");
                    } else {
                        connInfo = (Map) connectRef;
                    }

                    connector = ConnectorFactory.shared().getConnector(connInfo);

                    String templateName = (String) this.sequences.get(FlowTag.TEMPLATE.name());
                    String builtInTemplate = "";
                    if (templateName == null || templateName.isEmpty()) {
                        String method = (String) connInfo.get(ConnectorTag.CONNECTOR.name());
                        if ("FILE".equals(method)) {
                            builtInTemplate = "JSON";
                        } else {
                            builtInTemplate = method;
                        }
                    }

                    DocumentTemplate documentTemplate = DocumentTemplate.get(templateName, builtInTemplate, false);
                    connector.setDocumentTemplate(documentTemplate);

                    this.context.add(MODULE_CONTEXT.CONNECT_INFO, connInfo);
                    this.context.add(MODULE_CONTEXT.SEQUENCE, this.sequences);

                    if( this.flow instanceof FlowMockup ) {
                        Context channelContext = (Context) flowContext.get(Context.CONTEXT_KEY.CHANNEL_CONTEXT);

                        List<Map<String, Object>> itemList = (List) channelContext.get("ITEM");
                        List<Map<String, Object>> temp = new ArrayList<>();
                        Map<String, Object> one = itemList.get(0);
                        Map<String, Object> copy = new HashMap<>();
                        for( Map.Entry<String, Object> e : one.entrySet() ) {
                            copy.put(e.getKey(), "?");
                        }
                        temp.add(copy);

                        String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                        MappingRule rule = flowContext.getRules().get(ruleName);
                        String dbType = (String) connInfo.get(ConnectorTag.DBTYPE.toString());
                        rule.setFunctionProcessor(connector.getFunctionProcessor(dbType));

                        String query = documentTemplate.makeHeader(this.context, rule, temp);
                        List<String> param = rule.getParam();

                        for( String p : param ) {
                            query = query.replaceFirst("\\?", p);
                        }
                        channelContext.add("QUERY", query);
                        return;
                    }
                }

                connector.connect(flowContext, this);

                String action = (String) this.sequences.get(FlowTag.ACTION.name());

                switch(FlowTag.valueOf(action)) {
                    case CHECK:
                    {
                        connector.check();
                        flowContext.setCurrent(null);
                        setResult("success");
                        break;
                    }
                    case COUNT:
                    {
                        connector.count();
                        flowContext.setCurrent(null);
                        setResult("success");
                        break;
                    }
                    case READ:
                    {
                        String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                        MappingRule rule = flowContext.getRules().get(ruleName);

                        connector.beforeRead(rule);
                        break;
                    }
                    case CREATE:
                    {
                        String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                        MappingRule rule = flowContext.getRules().get(ruleName);

                        String dbType = (String) connInfo.get(ConnectorTag.DBTYPE.toString());
                        rule.setFunctionProcessor(connector.getFunctionProcessor(dbType));

                        connector.beforeCreate(rule);
                        break;
                    }
                    case DELETE:
                    {
                        connector.delete();
                        setResult("success");
                        break;
                    }
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            setResult("fail");
            setMsg(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public List processPartially(Flow flow, FlowContext flowContext, List buffer) throws Exception {
        try {
            String action = (String) this.sequences.get(FlowTag.ACTION.name());

            switch (FlowTag.valueOf(action)) {
                case READ:
                {
                    String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                    MappingRule rule = flowContext.getRules().get(ruleName);

                    Integer limit = PropertyMap.getIntegerDefault(this.sequences, FlowTag.BUFFERSIZE, 1);
                    //List part = connector.readPartially(rule);
                    connector.readBuffer(rule, buffer, limit);
                    if (buffer.isEmpty()) {
                        setResult("success");
                        buffer = null;
                        return null;
                    }

                    Boolean debug = (Boolean) this.sequences.get(FlowTag.DEBUG.name());
                    if (debug != null && debug ) {
                        logger.info(buffer);
                    }

                    return buffer;
                }
                case CREATE:
                    String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                    MappingRule rule = flowContext.getRules().get(ruleName);

                    if (buffer.isEmpty()) {
                        buffer = null;
                    }

                    connector.createPartially(buffer, rule);
                    if (buffer == null) {
                        setResult("success");
                        setMsg("");
                    }
                    return null;
                default:
                    return null;
            }
        } catch (Exception e) {
            connector.rollback();
            int errorPos = connector.getErrorPosition();
            setResult("fail");

            String errMsg = e.getMessage();
            if (errorPos >= 0) {
                errMsg = "#" + (errorPos + 1) + " : " + errMsg;
            }
            setMsg(errMsg);
            e.printStackTrace();
            throw e;
        }
    }

    public void process(Flow flow, FlowContext flowContext) throws Exception {
        try {
            String action = (String) this.sequences.get(FlowTag.ACTION.name());

            switch (FlowTag.valueOf(action)) {
                case READ:
                {
                    String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                    MappingRule rule = flowContext.getRules().get(ruleName);

                    resultList = connector.read(rule);

                    if( "BYPASS".equals(sequences.get(FlowTag.RESULT.name()))) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("ITEMS", resultList);
                        FGInputStream stream = new FGSharableInputStream(new JSONContainer(data, "HEADER", "ITEMS"));
                        flowContext.setCurrent(stream);

                        Boolean debug = (Boolean) this.sequences.get(FlowTag.DEBUG.name());
                        if (debug != null && debug) {
                            if (stream != null) {
                                Carrier carrier = stream.getCarrier();

                                Map temp = (Map) carrier.getSnapshot();
                                logger.info(temp);
                            }
                        }
                    } else {
                        flowContext.setCurrent(null);
                    }
                    break;
                }
                case CREATE:
                    String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                    MappingRule rule = flowContext.getRules().get(ruleName);

                    String dbType = (String) connInfo.get(ConnectorTag.DBTYPE.toString());
                    rule.setFunctionProcessor(connector.getFunctionProcessor(dbType));

                    Payload payload = null;

                    FGInputStream currentStream = flowContext.getCurrent();
                    if (currentStream != null) {
                        payload = flowContext.getCurrent().subscribe();
                    }

                    long sent = 0;
                    List itemList = new LinkedList<Map<String, Object>>();

                    Integer batchSize = PropertyMap.getIntegerDefault(this.sequences, FlowTag.BATCHSIZE, 1);
                    try {
                        while (payload.next() != -1) {
                            Object dataList = payload.getData();
                            long length = payload.getReadLength();

                            if (dataList instanceof List) {
                                @SuppressWarnings("unchecked")
                                List temp = (List) dataList;
                                itemList.addAll(temp);

                                if (batchSize > 0 && itemList.size() >= batchSize) {
                                    List sub = itemList.subList(0, batchSize);
                                    connector.create(sub, rule);
                                    sent += batchSize;
                                    sub.clear();
                                }
                            } else {
                                sent += length;
                            }
                        }

                        if (itemList.size() > 0) {
                            connector.create(itemList, rule);
                            sent += itemList.size();
                            itemList.clear();
                        }

                        flowContext.getCurrent().unsubscribe(payload);
                        flowContext.setCurrent(null);
                    } catch (Exception e) {
                        connector.rollback();
                        throw e;
                    }
                    break;
                default:
                    break;
            }

            setResult("success");
            setMsg("");
        } catch (Exception e) {
            setResult("fail");
            setMsg(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public void processAfter(Flow flow, FlowContext context) throws Exception {
        try {
            String action = (String) this.sequences.get(FlowTag.ACTION.name());

            switch (FlowTag.valueOf(action)) {
                case READ:
                {
                    connector.afterRead();

                    //flowContext.setCurrent(null);
                    break;
                }
                case CREATE:
                {
                    String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                    MappingRule rule = flowContext.getRules().get(ruleName);

                    String after = PropertyMap.getStringDefault(this.sequences, "AFTER", "COMMIT");
                    if ("COMMIT".equals(after)) {
                        connector.commit();
                    } else if ("ROLLBACK".equals(after)) {
                        connector.rollback();
                    }
                    connector.afterCreate(rule);

                    flowContext.setCurrent(null);
                    break;
                }
            }

            connector.close();

            String next = (String) this.sequences.get(FlowTag.CALL.name());
            flowContext.setNext(next);
        } catch (Exception e) {
            setResult("fail");
            setMsg(e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            if (!(flow instanceof FlowMockup)) {
                FloodgateHandlerManager.shared().handle(Step.MODULE_OUT, flow.getChannelContext(), this);
            }
        }
    }
}
