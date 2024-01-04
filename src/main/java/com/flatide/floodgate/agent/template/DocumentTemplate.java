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

import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;
import com.flatide.floodgate.agent.Context;
import com.flatide.floodgate.agent.flow.rule.MappingRule;
import com.flatide.floodgate.agent.flow.rule.MappingRuleItem;
import com.flatide.floodgate.agent.meta.MetaManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocumentTemplate {
    private static final Logger logger = LogManager.getLogger(DocumentTemplate.class);

    private static final Map<String, DocumentTemplate> templateCache = new ConcurrentHashMap<>();

    private TemplatePart root = null;

    private DocumentTemplate() {
    }

    /*
     * builtInTemplate : Pre-defined template located in scr/main/resources
     */
    public static DocumentTemplate get(String name, String builtInTemplate, boolean cache) throws Exception {
        if (name == null) {
            name = "";
        }

        DocumentTemplate documentTemplate = null;
        if (cache) {
            documentTemplate = templateCache.get(name);
        }

        if( documentTemplate == null || cache == false) {
            documentTemplate = new DocumentTemplate();

            List<String> lines = new ArrayList<>();

            if( !name.isEmpty() ) {
                String table = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_TEMPLATE);
                Map templateInfo = MetaManager.shared().read(table, name);
                Map templateData = (Map) templateInfo.get("DATA");
                if( templateData != null ) {
                    String read = (String) templateData.get("TEMPLATE");
                    String[] split = read.split("\\R");
                    for( String l : split ) {
                        lines.add(l);
                    }
                }
            } else {
                //not work for resources folder in executable jar
                //Path path = Paths.get(builtInTemplate + ".template");
                //List<String> lines = Files.readAllLines(path);

                URL url = Thread.currentThread().getContextClassLoader().getResource(builtInTemplate + ".template");
                Path path = Paths.get(url.getPath());
                lines = Files.readAllLines(path);
                
                /*
                ClassPathResource resource = new ClassPathResource(builtInTemplate + ".template");
                InputStreamReader isr = new InputStreamReader(resource.getInputStream(), "UTF-8");
                BufferedReader br = new BufferedReader(isr);

                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
                */
            }

            lines = preprocess(lines);

            documentTemplate.root = new TemplatePart("root", lines);
            //System.out.println(documentTemplate.root.print(0));

            templateCache.put(name, documentTemplate);
        }

        return documentTemplate;
    }

    /*
        template의 라인 연결(\)를 처리한다
     */
    private static List<String> preprocess(List<String> original) {
        List<String> lines = null;
        String buffer = "";
        boolean conjunction = false;
        if( original != null ) {
            lines = new ArrayList<>();
            for( String s : original ) {
                if( s.trim().equals("#else") ) {

                }
                if( s.trim().endsWith("\\")) {
                    if( !conjunction ) {
                        conjunction = true;
                        lines.add(buffer);
                        buffer = "";
                    }
                    s = s.substring(0, s.lastIndexOf("\\"));
                } else {
                    if( !conjunction ) {
                        lines.add(buffer);
                        buffer = "";
                    } else {
                        conjunction = false;
                    }
                }
                buffer += s;
            }
            if( buffer.length() > 0 ) {
                lines.add(buffer);
            }
            /*for( String s: lines) {
                System.out.println(s);
            }*/
        }

        return lines;
    }

    private TemplatePart getPart(String name) {
        if( this.root == null ) {
            return null;
        }
        return this.root.getPart(name);
    }

    public <T> String makeHeader(Context context, MappingRule rules, List<T> itemList) throws Exception {
        TemplatePart part = getPart("header");
        if( part == null ) {
            return "";
        }
        // header와 footer에서 첫번째 컬럼의 tag name등을 처리하기 위해
        T columnData = null;
        if( itemList != null ) {
            columnData = itemList.get(0);
        }
        return processPart(part, context, rules, itemList, columnData,0);
    }

    public <T> String makeFooter(Context context, MappingRule rules, List<T> itemList) throws Exception {
        TemplatePart part = getPart("footer");
        if( part == null ) {
            return "";
        }
        // header와 footer에서 첫번째 컬럼의 tag name등을 처리하기 위해
        T columnData = null;
        if( itemList != null ) {
            columnData = itemList.get(0);
        }
        return processPart(part, context, rules, itemList, columnData,0);
    }

    public <T> String makeBody(Context context, MappingRule rules, List<T> itemList, long index) throws Exception {
        TemplatePart part = getPart("body");
        if( part == null ) {
            return "";
        }
        return processPart(part, context, rules, itemList, null, index);
    }

    private <T> String processPart(TemplatePart part, Context context, MappingRule rules, List<T> itemList, T columnData, long index) throws Exception {
        StringBuilder builder = new StringBuilder();
        boolean isTrue = false;  // if else 구문을 위한 flag
        for(TemplatePart child : part.getChildren() ) {
            if( child.getName().isEmpty() ) {    // text only
                String content = child.getContent();
                content = context.evaluate(content);

                builder.append(content);
            } else {    // child Part
                if( child.getName().equals("row")) {
                    String row = processRow(child, context, rules, itemList, index);
                    builder.append(row);
                }
                if( child.getName().equals("br")) {
                    builder.append("\r\n");
                } else if( child.getName().equals("column")) {
                    String column = processColumn(child, context, columnData, rules);
                    builder.append(column);
                } else if( child.getName().equals("if")) {
                    String condition = child.getAttribute("condition");
                    if( condition == null) {
                        condition = "false";
                    }

                    if( context.evaluate(condition).equalsIgnoreCase("true") ) {
                        isTrue = true;
                        String result = processPart(child, context, rules, itemList, columnData, index);
                        builder.append(result);
                    } else {
                        isTrue = false;
                    }
                } else if( child.getName().equals("else")) {
                    if( !isTrue ) {
                        String result = processPart(child, context, rules, itemList, columnData, index);
                        builder.append(result);
                    }
                } else {
                    // skip
                }
            }
        }

        return builder.toString();
    }

    private <T> String processRow(TemplatePart part, Context context, MappingRule rules, List<T> itemList, long index) throws Exception {
        String rowDelimiter = part.getAttribute("delimiter");
        if( rowDelimiter == null) {
            rowDelimiter = "";
        }
        rowDelimiter = context.evaluate(rowDelimiter);
        rowDelimiter = rowDelimiter.replace("\\r", "\\s");
        rowDelimiter = rowDelimiter.replace("\\n", "\r\n");

        if( itemList != null ) {
            StringBuilder builder = new StringBuilder();

            for (T item : itemList) {
                if (index > 0) {
                    builder.append(rowDelimiter);
                }
                String applied = processPart(part, context, rules, itemList, item, index);
                builder.append(applied);
                index++;
            }

            return builder.toString();
        } else {
            return processPart(part, context, rules, null, null, 0);
        }
    }

    private <T> String processColumn(TemplatePart column, Context context, T columnData, MappingRule rules) throws Exception {
        StringBuilder builder = new StringBuilder();

        String delimiter = column.getAttribute("delimiter");
        delimiter = context.evaluate(delimiter);
        delimiter = delimiter.replace("\\r", "\\s");
        delimiter = delimiter.replace("\\n", "\r\n");

        for(TemplatePart part : column.getChildren() ) {
            if( part.getName().isEmpty() ) {
                String col = part.getContent();
                int i = 0;
                for (MappingRuleItem item : rules.getRules()) {
                    String result = col;
                    if (i > 0) {
                        builder.append(delimiter);
                    }

                    if( col.contains("$SOURCE$")) {
                        String source = rules.apply(item, columnData);
                        if( source != null ) {
                            source = processConditionalAttributes(column, context, source);

                            if (!"true".equals(column.getAttribute("ignoreType"))) {
                                if (item.getTargetType() != MappingRuleItem.RuleType.NUMBER) {
                                    source = "\"" + source + "\"";
                                }
                            }

                            if (source != null) {
                                result = result.replace("$SOURCE$", source);
                            }
                        }

                        if( source == null ) {
                            //logger.info(item.getSourceName() + " is not exist.");
                            //TODO Source가 없을 경우 empty로 계속 진행할 것인지 Exception을 발생시키고 중단할 것인지 결정할 것. 2021.03.15
                            result = "";
                            //throw new Exception(item.getSourceName() + " is not exist.");
                        }
                    }

                    result = result.replace("?SOURCE?", item.getSourceName());
                    result = result.replace("?TARGET?", item.getTargetName());

                    builder.append(result);
                    i++;
                }
            }   // 중첩 허용 안함
        }

        return builder.toString();
    }

    private String processConditionalAttributes(TemplatePart column, Context context, String value) {
        if( value == null ) {
            return null;
        }

        int j = -1;
        while( true ) {
            Set<String> findGroup = new HashSet<>();

            j++;
            String condition = column.getAttribute("if", j);
            if( condition.isEmpty()) {
                break;
            }

            condition = context.evaluate(condition);
            if( condition.startsWith("regx:")) {
                condition = condition.substring(5);

                Pattern pattern = Pattern.compile(condition);
                Matcher matcher = pattern.matcher(value);
                while (matcher.find()) {
                    findGroup.add(value.substring(matcher.start(), matcher.end()));
                }
                if (findGroup.size() == 0) {
                    continue;
                }
            } else if( !condition.equalsIgnoreCase("true") ) {
                continue;
            }

            String then = column.getAttribute("then", j);
            if( then.isEmpty()) {
                continue;
            }

            if( then.startsWith("replace:")) {
                then = then.substring(8);
                //TODO  다음과 같이 공백을 포함한 파라메터를 처리할 수 있도록 할 것 - 2021.03.08
                //      replace:$ 'pre $ post'
                String[] replaceParam = then.split(" ");
                // $를 현재 조작하고 있는 문자열로 변환
                replaceParam[0] = replaceParam[0].replace("$", value);
                replaceParam[1] = replaceParam[1].replace("$", value);

                if( replaceParam[0].equals("#") ) {
                    for( String f : findGroup) {
                        String rep = replaceParam[1].replace("#", f);
                        value = value.replace(f, rep);
                    }
                } else {
                    value = value.replace(replaceParam[0], replaceParam[1]);
                }
            } else if( then.startsWith("escape:")) {
                then = then.substring(7);
                then = then.replace("$", value);    // $를 현재 조작하고 있는 문자열로 변환

                if( then.equals("#") ) {
                    for( String f : findGroup) {
                        String rep;
                        switch(f) {
                            case "\n":
                                rep = "\\n";
                                break;
                            case "\r":
                                rep = "\\r";
                                break;
                            case "\t":
                                rep = "\\t";
                                break;
                            case "\b":
                                rep = "\\b";
                                break;
                            default:
                                rep = "\\" + f;
                                break;
                        }
                        value = value.replace(f, rep);
                    }
                } else {
                    value = value.replace(then, "\\" + then);
                }
            } else {
                then = then.replace("$", value);
                value = then;
            }
        }

        return value;
    }

}
