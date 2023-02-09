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

import java.io.*;

public class BytePipe implements Carrier {
    private BufferedInputStream inputStream;

    private byte[] buffer = null;
    private final int bufferSize;
    private int bufferReadSize = 0;

    private long size = 0;
    private long current = 0;

    private boolean isFinished = false;

    public BytePipe(String filepath) throws Exception {
        this(filepath, 8192);
    }

    private BytePipe(String filepath, int bufferSize) throws Exception {
        super();
        this.bufferSize = bufferSize;
        File file = new File(filepath);
        if( file.exists()) {
            this.size = file.length();
            this.inputStream = new BufferedInputStream(new FileInputStream(filepath), bufferSize);
        }
    }

    public BytePipe(InputStream inputStream, long size, int bufferSize) throws Exception {
        super();
        this.bufferSize = bufferSize;
        this.inputStream = new BufferedInputStream(inputStream, bufferSize);
        this.size = size;
    }

    @Override
    public void flushToFile(String filename) throws Exception {
    }

    @Override
    public Object getSnapshot() throws Exception {
        return null;
    }

    public void reset() {
        this.bufferReadSize = 0;
        this.current = 0;
    }

    public void close() {
        if( this.inputStream != null ) {
            try {
                this.inputStream.close();
            } catch( IOException e ) {
                e.printStackTrace();
            } finally {
                this.inputStream = null;
            }
        }
    }

    public long totalSize() {
        return this.size;
    }

    public long remainSize() {
        return this.size - this.current;
    }

    public Object getHeaderData() {
        return null;
    }

    public boolean isFinished() {
        return this.isFinished;
    }

    public Object getBuffer() {
        return this.buffer;
    }

    public long getBufferReadSize() {
        return this.bufferReadSize;
    }

    public long forward() throws Exception {
        try {
            // 멀티 쓰레드환경에서 데이타 오염을 방지하기 위해 새로운 객체를 생성
            this.buffer = new byte[this.bufferSize];

            this.bufferReadSize = this.inputStream.read(this.buffer);
            if( this.bufferReadSize == -1 ) {
                this.isFinished = true;
                this.inputStream.close();
            } else {
                this.current += this.bufferReadSize;
            }
            return this.bufferReadSize;
        } catch(IOException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
