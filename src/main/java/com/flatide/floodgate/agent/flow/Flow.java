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

package com.flatide.floodgate.agent.flow;

import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.Context.CONTEXT_KEY;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.system.utils.PropertyMap;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.flow.rule.MappingRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Flow {
    private final Context channelContext;
    protected FlowContext context;

    private final String flowId;
    private final String targetId;

    private String result;
    private String msg;

    public Context getChannelContext() {
        return channelContext;
    }

    public FlowContext getContext() {
        return context;
    }

    public String getFlowId() {
        return flowId;
    }

    public String getTargetId() {
        return targetId;
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

    public Flow(String targetId, Context context) {
        UUID id = UUID.randomUUID();

        this.flowId = id.toString();
        this.targetId = targetId;

        this.channelContext = context;
    }

    public Flow(String flowId, String targetId, Context context) {
        this.flowId = flowId;
        this.targetId = targetId;

        this.channelContext = context;
    }
    
    public void prepare(Map<String, Object> flowInfo, FGInputStream input) {
        this.context = new FlowContext(this.flowId, flowInfo);
        this.context.setCurrent(input);

        String method = channelContext.getString(Context.CONTEXT_KEY.HTTP_REQUEST_METHOD);
        Object entryMap = flowInfo.get(FlowTag.ENTRY.name());
        if( entryMap instanceof Map) {
            entryMap = (String)((Map<String, String>)entryMap).get(method);
        }
        this.context.setEntry((String) entryMap);

        this.context.setDebug((Boolean) flowInfo.get(FlowTag.DEBUG.name()));
        this.context.add(CONTEXT_KEY.CHANNEL_CONTEXT, channelContext);

        // Module
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> mods = (Map<String, Map<String, Object>>) flowInfo.get(FlowTag.MODULE.name());
        if (mods != null) {
            for( Map.Entry<String, Map<String, Object>> entry : mods.entrySet() ) {
                Module module = new Module( this, entry.getKey(), entry.getValue());
                this.context.getModules().put( entry.getKey(), module);
            }
        }

        // Connect Info
        /*HashMap<String, Object> connectInfoData = (HashMap) meta.get(FlowTag.CONNECT.name());
        if( connectInfoData != null ) {
            this.connectInfo = new ConnectInfo(connectInfoData);
        }*/

        // Rule
        @SuppressWarnings("unchecked")
        Map<String, Object> mappingData = (Map<String, Object>) flowInfo.get(FlowTag.RULE.name());
        //HashMap<String, Object> mappingData = (HashMap) meta.get(FlowTag.RULE.name());
        if( mappingData != null ) {
            for( Map.Entry<String, Object> entry : mappingData.entrySet() ) {
                MappingRule rule = new MappingRule();
                @SuppressWarnings("unchecked")
                Map<String, String> temp = (Map<String, String>) entry.getValue();
                rule.addRule( temp );
                this.context.getRules().put( entry.getKey(), rule );
            }
        }
    }

    public FGInputStream process() throws Exception {
        String entry = context.getString("CHANNEL_CONTEXT.REQUEST_PARAMS.entry");
        if( entry == null || entry.isEmpty() ) {
            entry = context.getEntry();
        }
        
        this.context.setNext(entry);
        while( this.context.hasNext()  ) {
            Module module = this.context.next();
            Module joinModule = null;

            try {
                module.processBefore(this, context);
                
                String joinTarget = PropertyMap.getStringDefault(module.getSequences(), FlowTag.PIPE, "");
                if (!joinTarget.isEmpty()) {
                    this.context.setNext(joinTarget);
                    if (this.context.hasNext()) {
                        joinModule = this.context.next();
                        joinModule.processBefore(this, context);
                    } else {
                        throw new Exception("Cannot find target module to pipe with.");
                    }

                    boolean complete = false;
                    
                    List<Map> buffer = new ArrayList<>();
                    while (!complete) {
                        //List part = module.processPartially(this, context, null);
                        module.processPartially(this, context, buffer);
                        if (buffer.isEmpty()) {
                            complete = true;
                        }
                        joinModule.processPartially(this, context, buffer);
                    }
                } else {
                    module.process(this, context);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            } finally {
                try {
                    module.processAfter(this, context);
                } finally {
                    if (joinModule != null) {
                        joinModule.processAfter(this, context);
                    }
                }
            }
        }

        return context.getCurrent();
    }
}
