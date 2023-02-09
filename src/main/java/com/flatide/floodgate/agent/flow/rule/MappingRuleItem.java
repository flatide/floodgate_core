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

package com.flatide.floodgate.agent.flow.rule;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MappingRuleItem {
    public enum RuleType {
        ANY,
        STRING,
        NUMBER,
        DATE
    }

    String sourceName;
    RuleType sourceType = RuleType.ANY;
    String sourceTypeSub = "";

    String targetName;
    RuleType targetType = RuleType.ANY;
    String targetTypeSub = "";

    public RuleAction action = RuleAction.reference;

    public enum RuleAction {
        system,
        reference,
        literal,
        function,
        order,
    }

    public MappingRuleItem(String target, String source) {
        this.sourceName = source.trim();
        this.targetName = target.trim();

        if( source.contains(":")) {
            String[] s = source.split(":");
            this.sourceName = s[0].trim();
            this.sourceType = RuleType.valueOf(s[1].trim());
            if( s.length > 2 ) {
                this.sourceTypeSub = s[2].trim();
            }
        }

        char first = this.sourceName.charAt(0);

        switch(first) {
            case '{':
                //{CONTEXT.REQUEST_PARAMS.totalcount}
                this.action = RuleAction.system;
                break;
            case '>':
                this.action = RuleAction.function;
                this.sourceName = this.sourceName.substring(1);
                break;
            case '+':
                this.action = RuleAction.literal;
                this.sourceName = this.sourceName.substring(1);
                break;
            case '#':
                this.action = RuleAction.order;
                this.sourceName = this.getSourceName().substring(1);
                break;
            default:
                break;
        }

        if( target.contains(":")) {
            String[] t = target.split(":");
            this.targetName = t[0].trim();
            this.targetType = RuleType.valueOf(t[1].trim());
            if( t.length > 2 ) {
                this.targetTypeSub = t[2].trim();
            }
        }

        if( this.sourceName.equals("=") ) {
            this.sourceName = this.targetName;
        }

        if( this.targetName.equals("=") ) {
            this.targetName = this.sourceName;
        }
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public RuleType getSourceType() {
        return sourceType;
    }

    public void setSourceType(RuleType sourceType) {
        this.sourceType = sourceType;
    }

    public String getTargetName() {
        return targetName;
    }

    public void setTargetName(String targetName) {
        this.targetName = targetName;
    }

    public RuleType getTargetType() {
        return targetType;
    }

    public void setTargetType(RuleType targetType) {
        this.targetType = targetType;
    }

    public String getSourceTypeSub() {
        return sourceTypeSub;
    }

    public void setSourceTypeSub(String sourceTypeSub) {
        this.sourceTypeSub = sourceTypeSub;
    }

    public String getTargetTypeSub() {
        return targetTypeSub;
    }

    public void setTargetTypeSub(String targetTypeSub) {
        this.targetTypeSub = targetTypeSub;
    }

    public RuleAction getAction() {
        return action;
    }

    public void setAction(RuleAction action) {
        this.action = action;
    }

    /*public <T> String process(T itemList) throws Exception {
        Object value = null;
        if (itemList != null) {
            switch (getAction()) {
                case reference:
                    value = ((Map)itemList).get(getSourceName());
                    break;
                case literal:
//                                다음과 같은 literal + reference의 복합형태를 처리한다
//
//                                "COL_VARCHAR" : "+nvl($CODE$, $CODE$, $SQ$)"
                    String source = getSourceName();

                    Pattern pattern = Pattern.compile("\\$.+?\\$");
                    Matcher matcher = pattern.matcher(source);

                    List<String> key = new ArrayList<>();
                    while (matcher.find()) {
                        key.add(source.substring(matcher.start() + 1, matcher.end() - 1));
                    }

                    for (String k : key) {
                        Object data;
                        data = ((Map) itemList).get(k);

                        if (data == null) {
                            data = "";
                        }
                        source = source.replace("$" + k + "$", String.valueOf(data));
                    }

                    value = source;
                    break;
                case function:
                    // template을 수정해야 하는 함수들을 먼저 처리
                    //v = ""; // v를 String type으로 강제함
                    value = preprocessFunc(this);
                    break;
                case order:
                    value = ((List)itemList).get(Integer.valueOf(getSourceName()));
                    break;
            }

            switch(getSourceType()) {
                case DATE:
                    String dateFormat = getSourceTypeSub();
                    SimpleDateFormat format = new SimpleDateFormat(dateFormat);
                    java.util.Date date = format.parse((String) value);
                    value = new java.sql.Date(date.getTime());
                    break;
                default:
                    break;
            }


            if (getTargetType() == MappingRuleItem.RuleType.DATE) {
                String dateFormat = getTargetTypeSub();
                SimpleDateFormat format = new SimpleDateFormat(dateFormat);

                value = format.format(value);
            }
        } else {    // Header나 Footer에서 Column 생성인 경우( JDBC PreparedStatement 생성)
            switch (getAction()) {
                case reference:
                    value = "?";
                    param.add(getSourceName());
                    break;
                case literal:
//                                    다음과 같은 literal + reference의 복합형태를 처리한다
//
//                                    "COL_VARCHAR" : "+nvl($CODE$, $CODE$, $SQ$)"

                    String source = getSourceName();

                    Pattern pattern = Pattern.compile("\\$.+?\\$");
                    Matcher matcher = pattern.matcher(source);

                    boolean find = false;
                    while (matcher.find()) {
                        find = true;
                        String key = source.substring(matcher.start() + 1, matcher.end() - 1);
                        param.add(key);
                    }

                    if( find ) {
                        source = source.replaceAll("\\$.+?\\$", "\\?");
                        value = source;
                    } else {
                        value = getSourceName();
                    }
                    break;
                case function:
                    // template을 수정해야 하는 함수들을 먼저 처리
                    Object v = processEmbedFunction(this);
                    if( v == null ) {
                        value = "";
                    } else {
                        value = v;
                    }
                    break;
                default:
                    break;
            }
        }
        return String.valueOf(value);
    }*/

}
