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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.Context.CONTEXT_KEY;
import com.flatide.floodgate.agent.flow.Flow;
import com.flatide.floodgate.agent.flow.module.Module;

public class FileLogHandler implements FloodgateAbstractHandler {
    private static final Logger logger = LogManager.getLogger(FileLogHandler.class);

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
}
