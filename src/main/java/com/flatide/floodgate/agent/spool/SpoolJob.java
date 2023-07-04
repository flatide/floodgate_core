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

import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;
import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.flow.Flow;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class SpoolJob implements Callable<Map> {
    private static final Logger logger = LogManager.getLogger(SpoolJob.class);

    String flowId;
    String target;
    Map<String, Object> flowInfo;
    FGInputStream current;
    Context context;

    public SpoolJob(String flowId, String target, Map<String, Object> flowInfo, FGInputStream current, Context context) {
        this.flowId = flowId;
        this.target = target;
        this.flowInfo = flowInfo;
        this.current = current;
        this.context = context;
    }

    @Override
    public Map call() {
        logger.info("Spooled Job " + flowId + " start in thread " + Thread.currentThread().getId());
        String spoolingPath = ConfigurationManager.shared().getString(FloodgateConstants.CHANNEL_SPOOLING_FOLDER);

        Map<String, Object> result = new HashMap<>();
        try {
            Flow flow = new Flow(flowId, this.target, context);
            flow.prepare(flowInfo, current);

            flow.process();

            result.put("result", "success");
            logger.info("Spooled Job " + flowId + " completed.");

            try {
                File file = new File(spoolingPath + "/" + flowId);
                file.delete();
            } catch( Exception e ) {
                e.printStackTrace();
            }
        } catch(Exception e) {
            e.printStackTrace();
            result.put("result", "fail");
            result.put("reason", e.getMessage());
            logger.info("Spooled Job " + flowId + " failed. : " + e.getMessage());
        }

        return result;
    }
}
