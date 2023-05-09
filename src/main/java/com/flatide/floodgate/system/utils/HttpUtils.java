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

package com.flatide.floodgate.system.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HttpUtils {
    private String url;
    private Map<String, String> params;
    private Map<String, String> properties;
    private int connectTimeout;
    private int readTimeout;

    private HttpUtils(Builder builder) {
        this.url = builder.url;
        this.params = builder.params;
        this.properties = builder.properties;
        this.connectTimeout = builder.connectTimeout;
        this.readTimeout = builder.readTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String url = "";
        private Map<String, String> params = new HashMap<>();
        private Map<String, String> properties = new HashMap<>();
        private int connectTimeout = 1000;
        private int readTimeout = 5000;

        private Builder() {
            this.properties.put("cache-control", "no-cache");
            this.properties.put("Accept", "application/json");
            this.properties.put("Content-Type", "application/json");
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder addParam(String key, String value) {
            this.params.put(key, value);
            return this;
        }

        public Builder addProperties(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public Builder setConnectTimeout(int mills) {
            this.connectTimeout = mills;
            return this;
        }

        public Builder setReadTimeout(int mills) {
            this.readTimeout = mills;
            return this;
        }

        public HttpUtils build() {
            return new HttpUtils(this);
        }
    }


    public String get() throws Exception {
        HttpURLConnection con = null;

        try {
            int i = 0;
            for( String key : this.params.keySet()) {
                if( i == 0 ) {
                    this.url += "?";
                } else {
                    this.url += "&";
                }
                String value = this.params.get(key) == null ? "" : URLEncoder.encode( this.params.get(key), "UTF-8" );
                this.url += (key + "=" + value);
                i++;
            }
            
            URL myurl = new URL(this.url);
            con = (HttpURLConnection) myurl.openConnection();

            con.setConnectTimeout(this.connectTimeout);
            con.setReadTimeout(this.readTimeout);
            con.setDoOutput(true);
            con.setRequestMethod("GET");

            for( String key : this.properties.keySet()) {
                con.setRequestProperty(key, this.properties.get(key));
            }

            int responseCode = con.getResponseCode();
            if( responseCode < 200 || responseCode > 299 ) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getErrorStream(), "UTF8"));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                System.err.println(response);
                return response.toString();
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF8"));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            /*ObjectMapper mapper = new ObjectMapper();

            Map<String, Object> map = mapper.readValue(response.toString(), new TypeReference<Map<String, Object>>() {});

            return map;*/
            return response.toString();
        } catch(Exception e) {
            throw e;
        } finally {
            if(con != null) try {con.disconnect();} catch(Exception e) {}
        }
    }

    public String post(Map body) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String jsonBody = mapper.writeValueAsString(body);
        return post(jsonBody);
    }

    public String post(String body) throws Exception {
        HttpURLConnection con = null;

        try {
            int i = 0;
            for( String key : this.params.keySet()) {
                if( i == 0 ) {
                    this.url += "?";
                } else {
                    this.url += "&";
                }
                String value = this.params.get(key) == null ? "" : URLEncoder.encode( this.params.get(key), "UTF-8" );
                this.url += (key + "=" + value);
                i++;
            }

            URL myurl = new URL(this.url);
            con = (HttpURLConnection) myurl.openConnection();

            con.setConnectTimeout(this.connectTimeout);
            con.setReadTimeout(this.readTimeout);
            con.setDoOutput(true);
            con.setRequestMethod("POST");

            for( String key : this.properties.keySet()) {
                con.setRequestProperty(key, this.properties.get(key));
            }

            if ( body != null) {
                OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream(), "UTF8");
                wr.write(body);
                wr.flush();
            }

            int responseCode = con.getResponseCode();
            if( responseCode < 200 || responseCode > 299 ) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getErrorStream(), "UTF8"));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                System.err.println(response);
                return response.toString();
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF8"));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            return response.toString();
        } catch(Exception e) {
            throw e;
        } finally {
            if(con != null) try {con.disconnect();} catch(Exception e) {}
        }
    }

    public String postFile(InputStream is, String fileName, int fileLength) throws Exception {
        String boundary = Long.toHexString(System.currentTimeMillis());
        String CRLF = "\r\n";

        HttpURLConnection con = null;

        try {
            int i = 0;
            for( String key : this.params.keySet()) {
                if( i == 0 ) {
                    this.url += "?";
                } else {
                    this.url += "&";
                }
                String value = this.params.get(key) == null ? "" : URLEncoder.encode( this.params.get(key), "UTF-8" );
                this.url += (key + "=" + value);
                i++;
            }

            URL myurl = new URL(this.url);
            con = (HttpURLConnection) myurl.openConnection();

            con.setConnectTimeout(this.connectTimeout);
            con.setReadTimeout(this.readTimeout);
            con.setDoOutput(true);
            con.setRequestMethod("POST");

            for( String key : this.properties.keySet()) {
                con.setRequestProperty(key, this.properties.get(key));
            }

            con.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
            try (OutputStream output = con.getOutputStream();
                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, "UTF-8"), true);
            ) {
                writer.append("--" + boundary).append(CRLF);
                writer.append("Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"")
                    .append(CRLF);
                writer.append("Content-Type: application/octet-stream").append(CRLF);
                // writer.append("Content-Transfer-Encoding: binary").append(CRLF);
                writer.append(CRLF).flush();

                byte[] allBytes = new byte[fileLength];
                is.read(allBytes);
                is.close();

                output.write(allBytes);
                output.flush();

                writer.append(CRLF).flush();
                writer.append("--" + boundary + "--").append(CRLF).flush();
            }

            int responseCode = con.getResponseCode();
            if( responseCode < 200 || responseCode > 299 ) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getErrorStream(), "UTF8"));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                System.err.println(response);
                return response.toString();
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF8"));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            return response.toString();
        } catch(Exception e) {
            throw e;
        } finally {
            if(con != null) try {con.disconnect();} catch(Exception e) {}
            if(is != null) try {is.close();} catch(Exception e) {}
        }
    }
}
