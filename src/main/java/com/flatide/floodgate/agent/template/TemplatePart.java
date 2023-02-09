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

package com.flatide.floodgate.agent.template;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TemplatePart {
    String name = "";
    Map<String, List<String>> attribute = new HashMap<>();
    String content = "";
    List<TemplatePart> children;

    public TemplatePart(String tag, List<String> lines) {
        if( tag == null || tag.isEmpty() ) {
            this.content = "";
            StringBuilder builder = new StringBuilder();
            for( String line : lines ) builder.append(line + "\n");
            this.content = builder.toString();

            this.content = this.content.substring(0, this.content.lastIndexOf("\n"));
            return;
        } else {
            tag = tag.trim();
            List<String> tags = tokenize(tag);
            if( tags.size() > 0 ) {
                this.name = tags.remove(0);
            }
            for(String t : tags) {
                String[] keyValue = t.split("=");
                if( keyValue.length != 2 ) {
                    System.out.println("Invalid Tag Parameter Format : "
                            + tag + " ### " + t);
                    continue;
                }
                List<String> values = this.attribute.get(keyValue[0]);
                if( values == null ) {
                    values = new ArrayList<>();
                }
                values.add(keyValue[1]);
                this.attribute.put(keyValue[0], values);
            }
        }

        this.children = new ArrayList<>();

        List<String> temp = new ArrayList<>();

        if( lines == null ) {
            return;
        }

        //  if~else~end 구조에서 else는 end와 else가 중첩되어 있는 것과 같으므로
        //  if를 처리할 때 flag를 설정하여 else가 한번은 end로, 한번은 else로
        //  처리될 수 있도록 한다.
        //  if~end~else~end의 축약형태를 지원하기 위함
        boolean expectElse = false;
        while( lines.size() > 0 ) {
            String cur = lines.remove(0);
            if (cur.trim().startsWith("#") ) {
                cur = cur.trim().substring(1);
                if( cur.trim().startsWith("#")) {   // # escape
                    temp.add(cur);
                } else if( cur.startsWith("else") && !expectElse) {
                    // if 블럭을 끝내고 else를 다시 처리하기 위해
                    lines.add(0, "#else");
                    break;
                } else if( cur.equals("end")) {
                    break;
                } else {
                    expectElse = false;

                    if(temp.size() > 0 ) {
                        TemplatePart part = new TemplatePart("", temp);
                        this.children.add(part);
                    }
                    TemplatePart part;
                    if( cur.equals("br")) {
                        part = new TemplatePart(cur, null);
                    } else {
                        if( cur.startsWith("if")) {
                            // else 블럭을 처리하기 위해
                            expectElse = true;
                        }
                        part = new TemplatePart(cur, lines);
                    }
                    this.children.add(part);
                    temp.clear();
                }
            } else {
                temp.add(cur);
            }
        }
        if (temp.size() > 0) {
            TemplatePart part = new TemplatePart("", temp);
            this.children.add(part);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getAttributes(String key) {
        return this.attribute.get(key);
    }

    public String getAttribute(String key) {
        return getAttribute(key, 0);
    }

    public String getAttribute(String key, int index) {
        List<String> value = this.attribute.get(key);
        String result = "";
        if( value != null ) {
            if( index < value.size() ) {
                result = value.get(index);
            }
        }
        return result;
    }

    public String getContent() {
        return content;
    }

    public List<TemplatePart> getChildren() {
        return children;
    }

    public TemplatePart getPart(String name) {
        for(TemplatePart part : this.children ) {
            if( part.getName().equals(name)) {
                return part;
            }
        }

        return null;
    }

    List<String> tokenize(String str) {
        int state = 0;

        int i = 0;
        StringBuilder token = new StringBuilder();
        List<String> result = new ArrayList<>();
        while(i < str.length()) {
            char ch = str.charAt(i);
            if( ch == '\\') {
                i++;
                if( i < str.length() ) {
                    char next = str.charAt(i);
                    if (next != '\\' && next != '\"' && next != '\'') {
                        if( next == 'n' || next == 'r' || next == 't' || next == 'b') {
                            switch(next) {
                                case 'n':
                                    token.append('\n');
                                    break;
                                case 'r':
                                    token.append('\r');
                                    break;
                                case 't':
                                    token.append('\t');
                                    break;
                                case 'b':
                                    token.append('\b');
                                    break;
                            }
                            i++;
                            continue;
                        }
                        else {
                            token.append(ch);
                        }
                    }
                    token.append(next);
                }
                i++;
                continue;
            }
            switch(state) {
                case 0: // normal
                    if (ch == '"') {
                        state = 1;
                    } else if (ch == '\'') {
                        state = 2;
                    } else if( ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
                        if( token.length() > 0 ) {
                            result.add(token.toString());
                        }
                        token = new StringBuilder();
                    } else {
                        token.append(ch);
                    }
                    break;
                case 1: // in Double Quotes
                    if( ch == '"') {
                        result.add(token.toString());
                        token = new StringBuilder();
                        state = 0;
                    } else {
                        token.append(ch);
                    }
                    break;
                case 2: // in Single Quotes
                    if( ch == '\'') {
                        result.add(token.toString());
                        token = new StringBuilder();
                        state = 0;
                    } else {
                        token.append(ch);
                    }
                    break;
            }

            i++;
        }
        if( token.length() > 0 ) {
            result.add(token.toString());
        }

        return result;
    }

    public String print(int depth) {
        String result = "";
        if( this.name == null || this.name.isEmpty() ) {
            result += this.content + "\n";
        } else {
            for(int i = 0; i < depth; i++ ) {
                result += "\t";
            }

            result += "#" + this.name + " " + this.attribute + "\n";
        }

        if( this.children != null ) {
            depth++;
            for (TemplatePart part : this.children) {
                result += part.print(depth);
            }
            depth--;
        }

        return result;
    }
}
