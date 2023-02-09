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

package com.flatide.floodgate.agent.flow.stream.carrier.pipe;

import com.flatide.floodgate.agent.flow.stream.carrier.Carrier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListPipe<T> implements Carrier {
    private Map header;
    private List<T> data;

    private List<T> buffer = null;
    private final int bufferSize;

    private int current = 0;

    private boolean isFinished = false;

    public ListPipe(Map header, List<T> data) {
        this(header, data, data.size());
    }

    public ListPipe(Map header, List<T> data, int bufferSize) {
        this.header = header;
        this.data = data;
        this.bufferSize = bufferSize;
    }

    @Override
    public void flushToFile(String filename) throws Exception {
    }

    @Override
    public Object getSnapshot() throws Exception {
        return null;
    }

    public void reset() {
        this.current = 0;
        this.isFinished = false;
    }

    public void close() {
        this.header = null;
        this.data = null;
    }

    public long totalSize() {
        return this.data.size();
    }

    public long remainSize() {
        return this.data.size() - this.current;
    }

    public Object getHeaderData() {
        return header;
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

    public long forward() {
        this.buffer = new ArrayList<>();

        for(int i = 0; i < this.bufferSize; i++) {
            if( this.current >= this.data.size() ) {
                break;
            }
            T data = this.data.get(this.current++);
            this.buffer.add(data);
        }

        if( this.buffer.size() == 0 ) {
            this.isFinished = true;
        }

        return this.buffer.size();
    }
}
