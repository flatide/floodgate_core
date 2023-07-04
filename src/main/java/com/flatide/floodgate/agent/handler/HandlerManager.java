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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.flatide.floodgate.agent.Context;

public final class HandlerManager {
    private static final Logger logger = LogManager.getLogger(HandlerManager.class);

    public enum Step {
        CHANNEL_IN,
        CHANNEL_OUT,
        FLOW_IN,
        FLOW_OUT,
        MODULE_IN,
        MODULE_OUT,
        MODULE_PROGRESS
    };

    private static final HandlerManager instance = new HandlerManager();

    private Map<String, FloodgateAbstractHandler> handlerList;

    private HandlerManager() {
        handlerList = new LinkedHashMap<>();
    }

    public static HandlerManager shared() {
        return instance;
    }

    public void addHandler(String name, FloodgateAbstractHandler handler) {
        if (handlerList.get(name) != null) {
            return;
        }
        handlerList.put(name, handler);
    }

    public void removeHandler(String name) {
        handlerList.remove(name);
    }

    public void handle(Step step, Context context, Object object) {
        for (Map.Entry<String, FloodgateAbstractHandler> entry : handlerList.entrySet()) {
            FloodgateAbstractHandler handler = entry.getValue();
            switch (step) {
                case CHANNEL_IN:
                handler.handleChannelIn(context, object);
                break;
                case CHANNEL_OUT:
                handler.handleChannelOut(context, object);
                break;
                case FLOW_IN:
                handler.handleFlowIn(context, object);
                break;
                case FLOW_OUT:
                handler.handleFlowOut(context, object);
                break;
                case MODULE_IN:
                handler.handleModuleIn(context, object);
                break;
                case MODULE_OUT:
                handler.handleModuleOut(context, object);
                break;
                case MODULE_PROGRESS:
                handler.handleModuleProgress(context, object);
                break;
            }
        }
    }
}
