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

package com.flatide.floodgate.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;
import com.flatide.floodgate.agent.flow.Flow;
import com.flatide.floodgate.agent.flow.FlowTag;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.agent.flow.stream.carrier.Carrier;
import com.flatide.floodgate.agent.handler.HandlerManager;
import com.flatide.floodgate.agent.handler.HandlerManager.Step;
import com.flatide.floodgate.agent.meta.MetaManager;
import com.flatide.floodgate.agent.spool.SpoolingManager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ChannelJob implements Callable<Map> {
    String target;
    Context context;
    FGInputStream current;

    public ChannelJob(String target, Context context, FGInputStream current) {
        this.target = target;
        this.context = context;
        this.current = current;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public FGInputStream getCurrent() {
        return current;
    }

    public void setCurrent(FGInputStream current) {
        this.current = current;
    }

    @Override
    public Map call() throws Exception {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> log = new HashMap<>();

        Flow flow = new Flow(this.target, this.context);

        try {
            // If FLOW exists in request body when API type is Instant Interfacing
            Map<String, Object> flowInfo = (Map) this.context.get(Context.CONTEXT_KEY.FLOW_META.toString());
            if( flowInfo == null ) {
                String flowInfoTable = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_FLOW);
                Map flowMeta = MetaManager.shared().read( flowInfoTable, target);
                flowInfo = (Map) flowMeta.get("DATA");
            }

            String flowId = flow.getFlowId();
            HandlerManager.shared().handle(Step.FLOW_IN, context, flow);

            Object spooling = flowInfo.get(FlowTag.SPOOLING.name());
            if( spooling != null && (boolean) spooling) {
                // backup ChannelJob with flowId
                String path = ConfigurationManager.shared().getString(FloodgateConstants.CHANNEL_SPOOLING_FOLDER);
                File folder = new File(path);
                if (!folder.exists()) {
                    folder.mkdir();
                }

                Map<String, Object> contextMap = this.context.getMap();

                Map<String, Object> newContext = new HashMap<>();
                for( Map.Entry<String, Object> entry : contextMap.entrySet() ) {
                    if(!Context.CONTEXT_KEY.REQUEST_BODY.name().equals(entry.getKey())) {
                        newContext.put(entry.getKey(), entry.getValue());
                    }
                }

                Map<String, Object> spoolInfo = new HashMap<>();
                spoolInfo.put("target", this.target);
                spoolInfo.put("context", newContext);

                ObjectMapper mapper = new ObjectMapper();
                mapper.writerWithDefaultPrettyPrinter().writeValue(new File(path + "/" + flowId), spoolInfo);

                SpoolingManager.shared().addJob(flowId);

                result.put("result", "spooled");
                result.put("ID", flowId.toString());
                log.put("RESULT", "spooled");
            } else {
                flow.prepare(flowInfo, current);
                FGInputStream returnStream = flow.process();
                if( returnStream != null ) {
                    Carrier carrier = returnStream.getCarrier();

                    result = (Map) carrier.getSnapshot();
                } else {
                    result.put("result", "success");
                    result.put("reason", "");
                }
                log.put("RESULT", "success");
                log.put("MSG", "");
            }
        } catch(Exception e) {
            e.printStackTrace();
            result.put("result", "fail");
            result.put("reason", e.getMessage());
            log.put("RESULT", "fail");
            log.put("MSG", e.getMessage());
        }

        flow.setResult((String) log.get("RESULT"));
        flow.setMsg((String) log.get("MSG"));

        HandlerManager.shared().handle(Step.FLOW_OUT, context, flow);

        return result;
    }
}
