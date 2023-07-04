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
import com.flatide.floodgate.agent.flow.Flow;
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.agent.meta.MetaManager;

import java.util.HashMap;
import java.util.Map;

public class PushAgent extends Spoolable {
    Context context;

    public PushAgent() {
        this.context = new Context();
    }

    public void addContext(String key, Object value) {
        this.context.add(key, value);
    }

    public Map<String, Object> process(FGInputStream data, String ifId) {

        // 페이로드 저장


        // 잡이 즉시처리인지 풀링인지 확인
        // 풀링일 경우
        // 즉시처리인 경우

        Map<String, Object> result = new HashMap<>();

        try {
            //String tableName = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_FLOW);
            //Map<String, Object> flowInfoResult = MetaManager.shared().read( tableName, ifId);
            //Map<String, Object> flowInfo = (Map<String, Object>) flowInfoResult.get("FLOW");

            // If FLOW exists in request body when API type is Instant Interfacing
            Map<String, Object> flowInfo = (Map) this.context.get(Context.CONTEXT_KEY.FLOW.toString());
            if( flowInfo == null ) {
                String flowInfoTable = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_FLOW);
                Map flowMeta = MetaManager.shared().read( flowInfoTable, ifId);
                flowInfo = (Map) flowMeta.get("DATA");
            }
            Flow flow = new Flow(ifId, this.context);
            flow.prepare(flowInfo, data);
            flow.process();
        } catch(Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}
