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
import com.flatide.floodgate.agent.flow.stream.FGInputStream;
import com.flatide.floodgate.system.utils.PropertyMap;
import com.flatide.floodgate.agent.flow.module.Module;
import com.flatide.floodgate.agent.flow.rule.MappingRule;

import java.util.Map;

/*
    Mockup for query generation
*/

public class FlowMockup extends Flow {
    public FlowMockup(String targetId, Context context) {
        super(targetId, context);
    }

    public FlowMockup(String flowId, String targetId, Context context) {
        super(flowId, targetId, context);
    }

    public FGInputStream process() throws Exception {
        String entry = this.context.getString("CONTEXT.REQUEST_PARAMS.entry");
        if (entry == null || entry.isEmpty()) {
            entry = this.context.getEntry();
        }

        this.context.setNext(entry);
        while (this.context.hasNext()) {
            Module module = this.context.next();

            try {
                module.processBefore(this, this.context);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        return this.context.getCurrent();
    }
}
