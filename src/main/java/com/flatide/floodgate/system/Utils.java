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

package com.flatide.floodgate.system;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static boolean checkFirewall(String url, int timeout) throws Exception {
        Socket socket = null;
        ArrayList<String> ip = new ArrayList<>();
        ArrayList<String> port = new ArrayList<>();
        String curIP = "";
        String curPort = "";
        try {
            if( url.contains("HOST") && url.contains("PORT") ) {    // Oracle tnsnames
                Pattern pattern = Pattern.compile("HOST\\s*=\\s*[\\w-]+\\.[\\w-]+\\.[\\w-]+\\.[\\w-]+");
                Matcher matcher = pattern.matcher(url);
                while( matcher.find() ) {
                    String f = matcher.group();
                    String[] temp = f.split("=");
                    ip.add( temp[1].trim() );
                }

                if( ip.size() == 0 ) {
                    throw new Exception("Cannot find proper ADDRESS from " + url);
                }

                pattern = Pattern.compile("PORT\\s*=\\s*[0-9]+");
                matcher = pattern.matcher(url);
                while( matcher.find() ) {
                    String f = matcher.group();
                    String[] temp = f.split("=");
                    port.add( temp[1].trim() );
                }
                if( port.size() != ip.size() ) {
                    throw new Exception("PORT not properly coupled with ADDRESS in " + url);
                }
            } else {
                Pattern pattern = Pattern.compile("[\\w-]+\\.[\\w-]+\\.[\\w-]+\\.[\\w-]+:[0-9]+");

                Matcher matcher = pattern.matcher(url);
                while( matcher.find() ) {
                    String f = matcher.group();
                    String[] temp = f.split(":");
                    ip.add( temp[0]);
                    port.add( temp[1]);
                }
                if( ip.size() == 0 ) {
                    throw new Exception("Cannot find proper IP:PORT from " + url);
                }
            }

            for( int i = 0; i < ip.size(); i++ ) {
                curIP = ip.get(i);
                curPort = port.get(i);

                socket = new Socket();
                socket.connect(new InetSocketAddress(curIP, Integer.parseInt(curPort)), timeout);
                socket.close();
                socket = null;
            }

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception( curIP + ":" + curPort + " : " + e.getMessage());
        } finally {
            try {
                if( socket != null ) socket.close();
            } catch(IOException e) {
            }
        }
    }

    public static boolean checkSocket(String ip, int port, int timeout) throws Exception {
        Socket socket = null;

        try {
            socket = new Socket();
            socket.connect(new InetSOcketAddress(ip.trim(), port), timeout);
            socket.close();
            socket = null;

            return true;
        } catch(IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if( socket != null ) socket.close();
            } catch(IOException e) {
            }
        }
    }
}
