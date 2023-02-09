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

package com.flatide.floodgate.agent.flow.stream.carrier.container;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flatide.floodgate.agent.flow.stream.carrier.Carrier;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JSONContainer implements Carrier {
    private final String headerTag;
    private final String dataTag;

    private final Map<String, Object> header = null;
    private Map<String, Object> data = null;
    private List<Object> buffer = null;

    private final int bufferSize;
    private final int bufferReadSize = -1;

    private boolean isFinished = false;

    public JSONContainer(Map<String, Object> data, String headerTag, String dataTag) throws Exception {
        this.headerTag = headerTag;
        this.dataTag = dataTag;
        this.buffer = (List<Object>) data.get(dataTag);
        this.bufferSize = this.buffer.size();

        this.data = data;
    }

    @Override
    public void flushToFile(String filename) throws Exception {
        try {
            String path = filename.substring(0, filename.lastIndexOf("/"));
            File folder = new File(path);

            if(!folder.exists()) {
                if( !folder.mkdir() ) {
                    throw new IOException("Cannot make folder " + path);
                }
            }

            ObjectMapper mapper = new ObjectMapper();
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(filename), data);
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public Object getSnapshot() throws Exception {
        return this.data;
    }

    public void reset() {
        this.isFinished = false;
    }

    public void close() {
    }

    public long totalSize() {
        return this.buffer.size();
    }

    public long remainSize() {
        if( this.isFinished ) {
            return 0;    // file offset 고려해 볼 것
        }
        return this.buffer.size();
    }

    public Object getHeaderData() {
        return this.data.get(this.headerTag);
    }

    public boolean isFinished() {
        return this.isFinished;
    }

    public Object getBuffer() {
        return this.buffer;
    }

    public long getBufferReadSize() {
        return this.buffer.size();
    }

    public long forward() throws Exception {
        if( this.isFinished ) {
            return -1;
        }
        this.isFinished = true;
        return this.buffer.size();
    }
}
