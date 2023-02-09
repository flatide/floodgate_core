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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MappingRule {
    private static final Logger logger = LogManager.getLogger(MappingRule.class);

    public enum Function {
        TARGET_DATE,
        DATE,
        SEQ
    }

    private final List<MappingRuleItem> rules = new ArrayList<>();
    FunctionProcessor functionProcessor = null;

    // for JDBC PreparedStatement
    private final List<String> param = new ArrayList<>();

    public List<String> getParam() {
        return this.param;
    }

    public void addRule(Map<String, String> rules) {
        for( Map.Entry<String, String> entry : rules.entrySet() ) {
            MappingRuleItem item = new MappingRuleItem(entry.getKey(), entry.getValue() );
            this.rules.add(item);
        }
    }

    /*public void addRule(String target, String source) {
        this.rules.add(new MappingRuleItem(target, source));
    }*/

    public List<MappingRuleItem> getRules() {
        return rules;
    }

    public void setFunctionProcessor(FunctionProcessor processor) {
        this.functionProcessor = processor;
    }

    public <T> String apply(MappingRuleItem item, T columnData) throws Exception {
        Object value = null;
        switch (item.getAction()) {
            case system:
                value = "?";
                param.add(item.getSourceName());
                break;
            case reference:
                value = ((Map)columnData).get(item.getSourceName());
                param.add(item.getSourceName());
                break;
            case literal:
                /*
                    다음과 같은 literal + reference의 복합형태도 처리해야 한다

                    "COL_VARCHAR" : "+nvl($CODE$, $CODE$, $SQ$)"
                 */
                String source = item.getSourceName();

                Pattern pattern = Pattern.compile("\\$.+?\\$");
                Matcher matcher = pattern.matcher(source);

                List<String> key = new ArrayList<>();
                while (matcher.find()) {
                    key.add(source.substring(matcher.start() + 1, matcher.end() - 1));
                }

                for (String k : key) {
                    Object data;
                    data = ((Map) columnData).get(k);

                    if (data == null) {
                        data = "";
                    }
                    source = source.replace("$" + k + "$", String.valueOf(data));
                    param.add(k);
                }

                value = source;
                break;
            case function:
                // template을 수정해야 하는 함수들을 먼저 처리
                //v = ""; // v를 String type으로 강제함
                if( this.functionProcessor != null ) {
                    value = this.functionProcessor.process(item);
                }

                if( value == "?" ) {
                    // jdbc template은 creating에서 다시 function을 평가해야 한다
                    param.add(">" + item.getSourceName());
                }
                break;
            case order:
                value = ((List)columnData).get(Integer.valueOf(item.getSourceName()));
                break;
        }

        if( value == null ) {
            return null;
        }

        try {
            switch (item.getSourceType()) {
                case DATE:
                    String dateFormat = item.getSourceTypeSub();
                    SimpleDateFormat format = new SimpleDateFormat(dateFormat);
                    java.util.Date date = format.parse((String) value);
                    value = new java.sql.Date(date.getTime());
                    break;
                default:
                    break;
            }
        } catch(IllegalArgumentException e) {
            logger.info(e.getMessage());
        }


        if (item.getTargetType() == MappingRuleItem.RuleType.DATE) {
            String dateFormat = item.getTargetTypeSub();
            SimpleDateFormat format = new SimpleDateFormat(dateFormat);

            try {
                value = format.format(value);
            } catch(IllegalArgumentException e) {
                // Ignore format error for jdbc template, '?'
                logger.info(e.getMessage());
            }
        }
        return String.valueOf(value);
    }

    /*
        preparedStatement에 파라메터로 들어갈 수 없는 SQL function등을 리턴한다

        processFuntionForTemplate으로 이름을 변경하면 좋을 것. 20201223
     */
    /*String preprocessFunc(MappingRuleItem item) {
        String func = item.getSourceName();
        Function function = Function.valueOf(func);

        switch (function) {
            case TARGET_DATE:
                //item.setAction(MappingRuleItem.RuleAction.literal);
                return "now()";
            default:
                return "?";
        }
    }*/
}
