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

package com.flatide.floodgate.agent.flow.stream;

public class Payload {
    private final FGInputStream current;
    private final int id;

    private Object data = null;
    private long dataSize = 0;

    Payload(FGInputStream current, int id) {
        this.current = current;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Object getData() {
        return this.data;
    }

    void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    // Total size, -1 if not possible
    public long    size() {
        return this.current.size();
    }

    private long    remains() {
        return this.current.remains(this);
    }

    public Object  getHeader() {
        return this.current.getHeader();
    }

    // whether whole data sent
    private boolean isFinished() {
        return remains() <= 0;
    }

    // blocking
    public long next() throws Exception {
        if(isFinished()) {
            this.dataSize = -1;
            return -1;
        }
        long length = this.current.next(this);
        if( length <= 0 ) {
            this.dataSize = -1;
            return -1;
        }

        return length;
    }

    public long getReadLength() {
        return dataSize;
    }
}
