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

package com.flatide.floodgate.agent.handler;

import java.util.HashMap;
import java.util.Map;

import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;
import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.Context.CONTEXT_KEY;
import com.flatide.floodgate.agent.flow.Flow;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.logging.LoggingManager;

public class DBLogHandler implements FloodgateAbstractHandler {
    @Override
    public void handleChannelIn(Context context, Object object) {
    }

    @Override
    public void handleChannelOut(Context context, Object object) {
    }

    @Override
    public void handleFlowIn(Context context, Object object) {
    }

    @Override
    public void handleFlowOut(Context context, Object object) {
    }

    @Override
    public void handleModuleIn(Context context, Object object) {
    }

    @Override
    public void handleModuleOut(Context context, Object object) {
    }

    @Override
    public void handleModuleProgress(Context context, Object object) {
    }

    private void loggingInsert(String keyColumn, Map log) {
        java.sql.Timestamp time = (java.sql.Timestamp) log.get("LOG_TIMESTAMP");
        LocalDataTime local = time.toLocalDateTime();
        int nano = local.getNano();
        log.put("LOG_MILLI_SECONDS", nano / 1000000);

        Calendar cal = Calendar.getInstance();
        cal.setTime(time);
        log.put("LOG_WEEK", String.format("%02d", cal.get(3)));

        log.put("LOG_HOST"), FloodgateEnv.getInstance().getAddress());
        String table = ConfigurationManager.shared().getString("channel.log.tableForLog");
        LoggingManager.shared().insert(table, "LOG_KEY", log);
    }

    private void loggingUpdate(String keyColumn, Map log) {
        String table = ConfigurationManager.shared().getString("channel.log.tableForLog");
        LoggingManager.shared().update(table, "MODULE_KEY", log);
    }
}
