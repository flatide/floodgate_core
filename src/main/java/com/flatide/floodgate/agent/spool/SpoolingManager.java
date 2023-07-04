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

package com.flatide.floodgate.agent.spool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;
import com.flatide.floodgate.agent.ChannelAgent;
import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.agent.flow.stream.FGSharableInputStream;
import com.flatide.floodgate.agent.flow.stream.carrier.container.JSONContainer;
import com.flatide.floodgate.agent.meta.MetaManager;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SpoolingManager {
    private static final Logger logger = LogManager.getLogger(SpoolingManager.class);

    private static final SpoolingManager instance = new SpoolingManager();

    LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

    ThreadPoolExecutor[] executor = new ThreadPoolExecutor[4];

    private SpoolingManager() {
        for (int i = 0; i < 4; i++) {
            this.executor[i] = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        }

        Thread thread = new Thread( () -> {
            logger.info("Spooling thread started...");
            String spoolingPath = ConfigurationManager.shared().getString(FloodgateConstants.CHANNEL_SPOOLING_FOLDER);
            //String flowInfoTable = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_FLOW);
            String payloadPath = ConfigurationManager.shared().getString(FloodgateConstants.CHANNEL_PAYLOAD_FOLDER);
            try {
                while (true) {
                    try {
                        long cur = System.currentTimeMillis();

                        String flowId = this.queue.take();

                        ObjectMapper mapper = new ObjectMapper();
                        @SuppressWarnings("unchecked")
                        Map<String, Object> spoolingInfo = mapper.readValue(new File(spoolingPath + "/" + flowId), LinkedHashMap.class);
                        String target = (String) spoolingInfo.get("target");

                        //Map<String, Object> flowInfoResult = MetaManager.shared().read(flowInfoTable, target);
                        //Map<String, Object> flowInfo = (Map<String, Object>) flowInfoResult.get("FLOW");

                        Map<String, Object> spooledContext = (Map<String, Object>) spoolingInfo.get("context");
                        // If FLOW exists in request body when API type is Instant Interfacing
                        Map<String, Object> flowInfo = (Map) spooledContext.get(Context.CONTEXT_KEY.FLOW.toString());
                        if( flowInfo == null ) {
                            String flowInfoTable = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_FLOW);
                            Map flowMeta = MetaManager.shared().read( flowInfoTable, target);
                            flowInfo = (Map) flowMeta.get("DATA");
                        }

                        String channel_id = (String) spooledContext.get(Context.CONTEXT_KEY.CHANNEL_ID.name());

                        @SuppressWarnings("unchecked")
                        Map<String, Object> payload = mapper.readValue(new File(payloadPath + "/" + channel_id), LinkedHashMap.class);
                        FGInputStream current = new FGSharableInputStream(new JSONContainer(payload, "HEADER", "ITEMS"));

                        spooledContext.put(Context.CONTEXT_KEY.REQUEST_BODY.name(), payload);
                        Context context = new Context();
                        context.setMap(spooledContext);


                        // 동일 target은 동일 쓰레드에서만 순차적으로 처리될 수 있도록 한다
                        int index = target.charAt(target.length() - 1) % 4;

                        SpoolJob job = new SpoolJob(flowId, target, flowInfo, current, context);
                        executor[index].submit(job);
                    } catch (InterruptedException e) {
                        throw e;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupted();
            }
        });

        thread.start();
    }

    public static SpoolingManager shared() {
        return instance;
    }

    public void addJob(String id) {
        this.queue.offer(id);
    }
}
