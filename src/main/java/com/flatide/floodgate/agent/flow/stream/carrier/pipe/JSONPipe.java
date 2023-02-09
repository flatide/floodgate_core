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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.flatide.floodgate.agent.flow.stream.carrier.Carrier;

import java.io.*;
import java.util.*;

public class JSONPipe implements Carrier {
    private final String headerTag;
    private final String dataTag;
    private JsonParser jParser;

    private Map<String, Object> header = null;
    private List<Object> buffer = null;
    private final int bufferSize;
    private int bufferReadSize = -1;

    //private long size = 0;
    private long remains = Integer.MAX_VALUE;

    private int status = 0;     // 0: start, 1: seek header or data, 2: in data, 99: error

    private boolean isFinished = false;

    public JSONPipe(InputStream inputStream, long size, String headerTag, String dataTag) throws Exception {
        this(inputStream, size, headerTag, dataTag,100);
    }

    public JSONPipe(InputStream inputStream, long size, String headerTag, String dataTag, int bufferSize) throws Exception {
        this.headerTag = headerTag;
        this.dataTag = dataTag;
        this.bufferSize = bufferSize;

        JsonFactory jfactory = new JsonFactory();

        this.jParser = jfactory.createParser(inputStream);
        //this.size = size;
    }

    @Override
    public void flushToFile(String filename) throws Exception {
    }

    @Override
    public Object getSnapshot() throws Exception {
        return null;
    }

    public void reset() {
        this.remains = Integer.MAX_VALUE;
        this.status = 0;
        this.isFinished = false;
    }

    public void close() {
        if( this.jParser != null ) {
            try {
                this.jParser.close();
            } catch( IOException e ) {
                e.printStackTrace();
            } finally {
                this.jParser = null;
            }
        }
    }

    public long totalSize() {
        return this.buffer.size();  // 실제 파일 사이즈 고려해 볼 것( 파일이 아닌 스트림일 경우 애매함)
    }

    public long remainSize() {
        return this.remains;    // file offset 고려해 볼 것
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

    public long forward() throws Exception {
        try {
            while( true ) {
                JsonToken token = null;
                if( this.status != 2) {
                    token = this.jParser.nextToken();

                    if( token == null ) {
                        this.isFinished = true;
                        this.remains = 0;
                        this.jParser.close();
                        break;
                    }
                }

                switch( this.status ) {
                    case 0:
                        if( token == JsonToken.START_OBJECT ) {
                            // JSON stream starts with with '{'
                            this.status = 1;
                        } else if( token == JsonToken.START_ARRAY ) {
                            // No header, no data tag, only data
                            this.status = 2;    // data seeking
                        }
                        break;
                    case 1: // in map
                        String name = this.jParser.getCurrentName();

                        if (name.equals(this.dataTag)) {
                            this.jParser.nextToken();
                            if( this.jParser.currentToken() != JsonToken.START_ARRAY ) {
                                throw new IllegalStateException("Data cannot be an object.");
                            }
                            this.status = 2;
                        } else {
                            if (this.headerTag == null || this.headerTag.isEmpty()) {
                                if (this.header == null) {
                                    this.header = new HashMap<>();
                                }
                                this.header.put(name, getChildAll());
                            } else {
                                if (name.equals(this.headerTag)) {
                                    try {
                                        this.header = (Map) getChildAll();
                                    } catch (Exception e) {
                                        throw new IllegalStateException("Header cannot be an array.");
                                    }
                                } else {
                                    // skip to next field
                                    skipChildAll();
                                }
                            }
                        }
                        break;
                    case 2: // data seeking
                        this.buffer = new ArrayList<>();
                        this.bufferReadSize = 0;
                        while(true) {
                            Object data = getChildAll();
                            if( data == null ) {
                                break;
                            } else {
                                this.buffer.add(data);
                                this.bufferReadSize++;
                                if( this.bufferReadSize == this.bufferSize) {
                                    break;
                                }
                            }
                        }

                        if( this.bufferReadSize == 0 ) {
                            this.isFinished = true;
                            this.remains = 0;
                            this.jParser.close();
                            return 0;
                        } else {
                            return this.bufferReadSize;
                        }
                }
            }
        } catch(Exception e) {
            this.isFinished = true;
            this.remains = 0;
            try{ this.jParser.close(); } catch(Exception e1) {e.printStackTrace();}
            throw e;
        }
        return this.bufferReadSize;
    }

    // 현재 토큰의 하부구조를 모두 가져온다
    // 성능을 위해 non-recursive로 구현
    private Object getChildAll() throws Exception {
        int depth = -1;

        String fieldName = "";
        Object current = null;

        List<Object> parentList = new ArrayList<>();
        while(true) {
            JsonToken token = this.jParser.nextToken();

            if( token.isStructStart() ) {
                depth++;

                Object child;
                if(token == JsonToken.START_OBJECT ) {
                    child = new HashMap<String, Object>();
                } else {
                    child = new ArrayList<>();
                }

                if( current != null ) {
                    if(current instanceof Map) {
                        ((Map<String, Object>) current).put(fieldName, child);
                    } else {
                        ((List<Object>) current).add(child);
                    }
                    parentList.add(depth, current);
                } else {
                    parentList.add(depth, child);
                }

                current = child;

                continue;
            }
            if( token.isStructEnd() ) {
                if( depth == -1 ) {
                    return current;
                }

                current = parentList.remove(depth);

                depth--;
                if( depth == -1 ) {
                    return current;
                }

                continue;
            }

            if( token == JsonToken.FIELD_NAME) {
                fieldName = this.jParser.getCurrentName();
            } else {
                Object value = null;
                switch(token) {
                    case VALUE_STRING:
                        value = this.jParser.getText();
                        break;
                    case VALUE_TRUE:
                        value = true;
                        break;
                    case VALUE_FALSE:
                        value = false;
                        break;
                    case VALUE_NUMBER_INT:
                        value = this.jParser.getIntValue();
                        break;
                    case VALUE_NUMBER_FLOAT:
                        value = this.jParser.getFloatValue();
                        break;
                    case VALUE_NULL:
                        break;
                    case VALUE_EMBEDDED_OBJECT:
                        value = this.jParser.getEmbeddedObject();
                        break;
                }

                if( current == null ) {
                    return value;
                } else if( current instanceof Map) {
                    ((Map<String, Object>)current).put(fieldName, value);
                } else {
                    ((List<Object>)current).add(value);
                }
            }
        }
    }

    private void skipChildAll() throws Exception {
        int depth = 0;

        while(true) {
            JsonToken token = this.jParser.nextToken();

            if( token.isStructStart() ) {
                depth++;
            } else if( token.isStructEnd() ) {
                depth--;
            }

            if( depth == 0 ) {
                return;
            }
        }
    }
}
