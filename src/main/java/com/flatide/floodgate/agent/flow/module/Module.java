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
import com.flatide.floodgate.agent.connector.ConnectorTag;
import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.connector.ConnectorBase;
import com.flatide.floodgate.agent.flow.Flow;
import com.flatide.floodgate.agent.flow.FlowContext;
import com.flatide.floodgate.agent.flow.FlowMockup;
import com.flatide.floodgate.agent.flow.FlowTag;
import com.flatide.floodgate.agent.connector.ConnectorFactory;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.agent.flow.stream.FGSharableInputStream;
import com.flatide.floodgate.agent.flow.stream.Payload;
import com.flatide.floodgate.agent.flow.stream.carrier.container.JSONContainer;
import com.flatide.floodgate.agent.flow.rule.MappingRule;
import com.flatide.floodgate.agent.meta.MetaManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Module {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    private static final Logger logger = LogManager.getLogger(Module.class);

    private final String id;
    private final String name;
    private final Map<String, Object> sequences;
    private final Flow flow;

    private Integer progress = 0;

    private String result;
    private String msg;

    public Module(Flow flow, String name, Map<String, Object> data) {
        this.flow = flow;
        this.name = name;
        this.sequences = data;

        UUID id = UUID.randomUUID();
        this.id = id.toString();
    }

    public String getName() {
        return name;
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
    public void processBefore(FlowContext flowContext) {
    }

    public void process(FlowContext flowContext) throws Exception {
        if (this.sequences != null) {
            ConnectorBase connector;

            Object connectRef = this.sequences.get(FlowTag.CONNECT.name());
            Map connInfo;
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
                if( templateName == null || templateName.isEmpty()) {
                    String method = (String) connInfo.get(ConnectorTag.CONNECTOR.name());
                    if ("FILE".equals(method)) {
                        builtInTemplate = "JSON";
                    } else {
                        builtInTemplate = method;
                    }
                }

                DocumentTemplate documentTemplate = DocumentTemplate.get(templateName, builtInTemplate, false);
                connector.setDocumentTemplate(documentTemplate);

                flowContext.add("CONNECT_INFO", connInfo);
                flowContext.add("SEQUENCE", this.sequences);
                try {
                    if( this.flow instanceof FlowMockup ) {
                        Context context = (Context) flowContext.get(Context.CONTEXT_KEY.CHANNEL_CONTEXT);

                        List<Map<String, Object>> itemList = (List) context.get("ITEM");
                        List<Map<String, Object>> temp = new ArrayList<>();
                        Map<String, Object> one = itemList.get(0);
                        Map<String, Object> copy = new HashMap<>();
                        for( Map.Entry<String, Object> e : one.entrySet() ) {
                            copy.put(e.getKey(), "?");
                        }
                        temp.add(copy);

                        String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                        MappingRule rule = flowContext.getRules().get(ruleName);
                        String dbType = (String) connInfo.get(ConnectorTag.JDBCTag.DBTYPE.toString());
                        rule.setFunctionProcessor(connector.getFunctionProcessor(dbType));

                        String query = documentTemplate.makeHeader(flowContext, rule, temp);
                        List<String> param = rule.getParam();

                        for( String p : param ) {
                            query = query.replaceFirst("\\?", p);
                        }
                        context.add("QUERY", query);
                        return;
                    }

                    connector.connect(flowContext, this);

                    String action = (String) this.sequences.get(FlowTag.ACTION.name());

                    switch(FlowTag.valueOf(action)) {
                        case CHECK:
                        {
                            connector.check();
                            flowContext.setCurrent(null);
                            break;
                        }
                        case COUNT:
                        {
                            connector.count();
                            flowContext.setCurrent(null);
                            break;
                        }
                        case READ:
                        {
                            String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                            MappingRule rule = flowContext.getRules().get(ruleName);
                            List result = connector.read(rule);

                            if("BYPASS".equals(sequences.get(FlowTag.RESULT.name()))) {
                                Map<String, Object> data = new HashMap<>();
                                data.put("ITEMS", result);
                                FGInputStream stream = new FGSharableInputStream( new JSONContainer(data, "HEADER", "ITEMS") );
                                flowContext.setCurrent(stream);
                            } else {
                                flowContext.setCurrent(null);
                            }
                            break;
                        }
                        case CREATE:
                            String ruleName = (String) this.sequences.get(FlowTag.RULE.name());
                            MappingRule rule = flowContext.getRules().get(ruleName);

                            String dbType = (String) connInfo.get(ConnectorTag.JDBCTag.DBTYPE.toString());
                            rule.setFunctionProcessor(connector.getFunctionProcessor(dbType));

                            Payload payload = null;

                            FGInputStream currentStream = flowContext.getCurrent();
                            if( currentStream != null ) {
                                payload = flowContext.getCurrent().subscribe();
                                //Payload payload = context.getPayload();
                            }
                            //logger.info(data.toString());
                            connector.create(payload, rule);

                            flowContext.getCurrent().unsubscribe(payload);
                            flowContext.setCurrent(null);
                            break;
                        case DELETE:
                            connector.delete();
                            break;
                        default:
                            break;
                    }

                    String next = (String) this.sequences.get(FlowTag.CALL.name());
                    flowContext.setNext(next);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    connector.close();
                }
            }
        }
    }

    /*
        FlowContext의 output에 대한 처리
     */
    public void processAfter(FlowContext context) {
    }
}
