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

import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.agent.flow.stream.carrier.Carrier;
import com.flatide.floodgate.agent.logging.LoggingManager;
import com.flatide.floodgate.agent.meta.*;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ChannelAgent {
    private static final Logger logger = LogManager.getLogger(ChannelAgent.class);

    private Context context;

    public ChannelAgent() {
        this.context = new Context();
    }

    public void addContext(Context.CONTEXT_KEY key, Object value) {
        this.context.add(key.name(), value);
    }

    public Object getContext(Context.CONTEXT_KEY key) {
        return this.context.get(key.name());
    }

    public Object getContext(String key) { return this.context.get(key); }

    public Map<String, Object> process(FGInputStream current, String api) throws Exception {
        // Unique ID 생성
        UUID id = UUID.randomUUID();
        addContext(Context.CONTEXT_KEY.CHANNEL_ID, id.toString());

        addContext(Context.CONTEXT_KEY.API, api);

        // API 정보 확인
        String apiTable = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_API);
        Map apiMeta = MetaManager.shared().read( apiTable, api);
        Map apiInfo = (Map) apiMeta.get("DATA");

        Map<String, Object> log = new HashMap<>();

        //Date startTime = new Date(System.currentTimeMillis());
        long start = System.currentTimeMillis();
        java.sql.Timestamp startTime = new java.sql.Timestamp(start);
        log.put("ID", id.toString());
        log.put("API_ID", api);
        log.put("START_TIME", startTime);
        String historyTable = ConfigurationManager.shared().getString(FloodgateConstants.CHANNEL_LOG_TABLE_FOR_API);
        LoggingManager.shared().insert(historyTable, "ID",  log);



        /*
            "TARGET": {
                "": ["IF_ID1", "IF_ID2"],
                "CD0001": ["IF_ID3"] }
         */
        Map<String, List<String>> targetMap = (Map) apiInfo.get("TARGET");

        List<String> targetList = new ArrayList<>();

        Map params = (Map) getContext(Context.CONTEXT_KEY.REQUEST_PARAMS);
        String targets = (String) params.get("targets");
        if( targets != null && !targets.isEmpty() ) {
            String[] split = targets.split(",");
            for( String t : split ) {
                List<String> group = targetMap.get(t);
                if( group != null ) {
                    targetList.addAll(group);
                } else {
                    targetList.add(t.trim().toUpperCase());
                }
            }
        } else {
            // 아래 두가지 모두 가능
            targetMap.values().stream().forEach(targetList::addAll);
            //targetList = targetMap.values().stream().flatMap(x -> x.stream()).collect(Collectors.toList());
        }

        logger.debug(targetList);

        // 페이로드 저장 true인 경우
        // 페이로드 저장은 호출시의 데이타에 한정한다
        if( (boolean) apiInfo.get("BACKUP_PAYLOAD") == true) {
            Carrier carrier = current.getCarrier();
            try {
                String path = ConfigurationManager.shared().getString(FloodgateConstants.CHANNEL_PAYLOAD_FOLDER);
                carrier.flushToFile(path + "/" + id.toString());
            } catch(Exception e) {

            }
        }

        Map<String, Object> result = new HashMap<>();

        String logString = "";
        try {
            String flowInfoTable = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_FLOW);
            Map<String, Object> concurrencyInfo = (Map<String, Object>) apiInfo.get("CONCURRENCY");

            // 병렬실행인 경우
            if( concurrencyInfo != null && (boolean) concurrencyInfo.get("ENABLE") == true) {
                System.out.println("Thread Max: " + concurrencyInfo.get("THREAD_MAX"));

                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

                Future<Map>[] futures = new Future[targetList.size()];;

                int i = 0;
                for( String target : targetList) {
                    //TODO current를 복제해서 사용할 것 2021.07.06
                    ChannelJob job = new ChannelJob(target, this.context, current);
                    futures[i] = executor.submit(job);
                    i++;
                }

                i = 0;
                for( String target : targetList) {
                    result.put(target, futures[i].get());
                    i++;
                }

            } else {
                for (String target : targetList) {
                    //Map<String, Object> flowInfo = MetaManager.shared().get(flowInfoTable, target);
                    ChannelJob job = new ChannelJob(target, this.context, current);

                    try {
                        result.put(target, job.call());
                    } catch(Exception e ) {
                        e.printStackTrace();
                        result.put(target, e.getMessage());
                    }
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            logString = e.getMessage();
        }

        //Date endTime = new Date(System.currentTimeMillis());
        long end = System.currentTimeMillis();
        java.sql.Timestamp endTime = new java.sql.Timestamp(end);

        log.put("ID", id.toString());
        log.put("END_TIME", endTime);
        log.put("LOG", logString);
        LoggingManager.shared().update(historyTable, "ID", log);
        logger.info(String.format("%s (%s) is done : %s ms", id, api, end - start));
        return result;
    }
}
