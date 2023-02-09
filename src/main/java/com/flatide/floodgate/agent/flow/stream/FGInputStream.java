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

import com.flatide.floodgate.agent.flow.stream.carrier.Carrier;

import java.util.ArrayList;

public abstract class FGInputStream {
    protected final Carrier carrier;
    private int maxSubscriber = 1;
    private int currentSubscriber = 0;
    private int countOfCurrentDone = Integer.MAX_VALUE;

    private final ArrayList<Payload> payloads =  new ArrayList<>();

    private Object currentData = null;
    private long currentSize = 0;

    public FGInputStream(Carrier carrier) {
        this.carrier = carrier;
    }

    public Carrier getCarrier() {
        return carrier;
    }

    int getMaxSubscriber() {
        return maxSubscriber;
    }

    int getCurrentSubscriber() {
        return currentSubscriber;
    }

    public void setCurrentSubscriber(int currentSubscriber) {
        this.currentSubscriber = currentSubscriber;
    }

    public int getCountOfCurrentDone() {
        return countOfCurrentDone;
    }

    public void setCountOfCurrentDone(int countOfCurrentDone) {
        this.countOfCurrentDone = countOfCurrentDone;
    }

    public void increaseCountOfCurrentDone() {
        this.countOfCurrentDone++;
    }

    public void reset() {
        this.carrier.reset();
    }

    public void close() {
    }

    public void setMaxSubscriber(int maxSubscriber) {
        this.maxSubscriber = maxSubscriber;
    }

    public synchronized Payload subscribe() {
        Payload payload = new Payload(this, this.currentSubscriber++);
        this.payloads.add(payload);
        return payload;
    }

    public synchronized void unsubscribe(Payload payload)
    {
        int id = payload.getId();
        this.payloads.remove(id);
        this.currentSubscriber--;
    }

    public long size() {
        return this.carrier.totalSize();
    }

    public long remains(Payload payload) {
        return this.carrier.remainSize();
    }

    public Object  getHeader() {
        return this.carrier.getHeaderData();
    }

    public long next(Payload payload) throws Exception {
        if( this.carrier.isFinished() ) {
            return -1;
        }

        if( this.countOfCurrentDone >= this.currentSubscriber ) {
            this.currentSize = this.carrier.forward();

            this.currentData = this.carrier.getBuffer();
            this.countOfCurrentDone = 0;
        }

        payload.setData(this.currentData);
        payload.setDataSize(this.currentSize);

        this.countOfCurrentDone++;

        return this.currentSize;
    }
}
