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

package com.flatide.floodgate.system.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flatide.floodgate.ConfigurationManager;
import com.flatide.floodgate.FloodgateConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FDataSourceFile extends FDataSourceDefault {
    // NOTE spring boot의 logback을 사용하려면 LogFactory를 사용해야 하나, 이 경우 log4j 1.x와 충돌함(SoapUI가 사용)
    private static final Logger logger = LogManager.getLogger(FDataSourceFile.class);

    String path;
    String type;

    //HashMap<String, String> filenameRule = new HashMap<>();

    public FDataSourceFile(String name) {
        super(name);

        this.path = ".";
        this.type = ConfigurationManager.shared().getString("datasource." + name + ".format");
    }

    /*public MetaSourceFile(String path, String type) {
        this.path = path;
        this.type = type;
    }*/

    @Override
    public boolean connect() {
        File path = new File(this.path);
        return path.exists();
    }


    @Override
    public String getName() {
        return "FILE";
    }

    @Override
    public List<String> getAllKeys(String tableName, String keyColumn) throws Exception {
        List<String> result = new ArrayList<>();

        String filename = makeFilename(tableName, ".+");    // <ID>가 있다면 .+으로 대체

        if( isMultipleFile(tableName) ) {
            String p = filename.substring(0, filename.lastIndexOf("/"));
            final String rule = filename.substring(filename.lastIndexOf("/") + 1);
            File f = new File(this.path + "/" + p);

            FilenameFilter filter = (dir, name) -> {
                Pattern pattern = Pattern.compile(rule);

                Matcher matcher = pattern.matcher(name);
                if( matcher.matches()) {
                    logger.debug(name + " : " + matcher.matches());
                }
                return matcher.matches();
            };

            String[] files = f.list(filter);

            if( files != null ) {
                int prefixLength = rule.indexOf(".+");
                int postfixLength = rule.length() - (prefixLength + 2);
                for( String file : files ) {
                    file = file.substring(prefixLength);
                    file = file.substring(0, file.length() - postfixLength);

                    result.add(file);
                }
            }
        } else {
            Map<String, Object> data = readJson(this.path + "/" + filename);

            for (Object key : data.keySet()) {
                result.add((String) key);
            }
        }

        return result;
    }

    @Override
    public boolean create(String key) {
        return false;
    }

    /*
    @Override
    public Map<String, Object> readData(String tableName, String keyColumn, String key) throws Exception {
        String filename = this.path + "/" + makeFilename(tableName, key);
        Map<String, Object> data = readJson(filename);

        if( isMultipleFile(tableName) ) {
            return data;
        } else {
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) data.get(key);
            return row;
        }
    }

    @Override
    public boolean insertData(String tableName, String keyColumn, String key, Map<String, Object> row) throws Exception {
        String filename = this.path + "/" + makeFilename(tableName, key);
        if( isMultipleFile(tableName)) {
            File file = new File(filename);
            if( file.exists() ) {
                return false;   // key is duplicated
            }

            writeJson(filename, row);
        } else {
            Map<String, Object> data;
            File file = new File(filename);
            if( !file.exists()) {
                data = new HashMap<>();
            } else {
                data = readJson(filename);
                if( data == null ) {
                    data = new HashMap<>();
                }
            }
            if( data.get(key) == null ) {
                data.put(key, row);
                writeJson(filename, data);
            } else {
                return false;   // key is duplicated
            }
        }

        return true;
    }

    @Override
    public boolean updateData(String tableName, String keyColumn, String key, Map<String, Object> row) throws Exception {
        String filename = this.path + "/" + makeFilename(tableName, key);
        if( isMultipleFile(tableName) ) {
            File file = new File(filename);
            if( !file.exists()) {
                return false;   // key is not exist
            }

            writeJson(filename, row);
        } else {
            File file = new File(filename);
            if( !file.exists()) {
                return false;   // key is not exist
            }

            Map<String, Object> data = readJson(filename);
            if( data == null || data.get(key) == null) {
                return false;   // key is not exist
            }

            data.put(key, row);
            writeJson(filename, data);
        }

        return true;
    }

    @Override
    public boolean deleteData(String tableName, String keyColumn, String key, boolean backup) throws Exception {
        String filename = this.path + "/" + makeFilename(tableName, key);

//        File folder = new File(this.path + "/" + config.getSource().get("backupFolder"));
//        if( folder.exists() == false ) {
//            folder.mkdir();
//        }

        String backupFilename = this.path + "/" + ConfigurationManager.shared().getConfig().getMeta().get("source.backupFolder") + "/" + makeBackupFilename(tableName, key);
        File backupFile = new File(backupFilename);
        if( backupFile.exists()) {
            backupFilename += "." + UUID.randomUUID();
        }

        if (isMultipleFile(tableName)) {
            File file = new File(filename);
            if( !file.exists()) {
                return false;
            }

            Map<String, Object> data = readJson(filename);
            if( data == null ) {
                return false;   // key is not exist
            }

            if( !file.delete() ) {
                throw new IOException("Cannot delete file " + filename);
            }

            if( backup ) {
                //file.renameTo(new File(backupFilename));
                writeJson(backupFilename, data);
            }
        } else {
            File file = new File(filename);
            if( !file.exists()) {
                return false;   // key is not exist
            }

            Map<String, Object> data = readJson(filename);
            if( data == null || data.get(key) == null) {
                return false;   // key is not exist
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) data.get(key);
            if( backup ) {
                Map<String, Object> backupData = new HashMap<>();
                backupData.put(key, row);

                writeJson(backupFilename, backupData);
            }
            data.remove(key);
            writeJson(filename, data);
        }

        return true;
    }*/

    @Override
    public Map<String, Object> read(String tableName, String keyColumn, String key) throws Exception {
        String filename = this.path + "/" + makeFilename(tableName, key);
        Map<String, Object> data = readJson(filename);

        if( isMultipleFile(tableName) ) {
            return data;
        } else {
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) data.get(key);
            return row;
        }
    }

    @Override
    public boolean insert(String tableName, String keyColumn, Map<String, Object> row) throws Exception {
        String key = (String) row.remove(keyColumn);
        String filename = this.path + "/" + makeFilename(tableName, key);
        if( isMultipleFile(tableName)) {
            File file = new File(filename);
            if( file.exists() ) {
                return false;   // key is duplicated
            }

            writeJson(filename, row);
        } else {
            Map<String, Object> data;
            File file = new File(filename);
            if( !file.exists()) {
                data = new HashMap<>();
            } else {
                data = readJson(filename);
                if( data == null ) {
                    data = new HashMap<>();
                }
            }
            if( data.get(key) == null ) {
                data.put(key, row);
                writeJson(filename, data);
            } else {
                return false;   // key is duplicated
            }
        }

        return true;
    }

    @Override
    public boolean update(String tableName, String keyColumn, Map<String, Object> row) throws Exception {
        String key = (String) row.remove(keyColumn);
        String filename = this.path + "/" + makeFilename(tableName, key);
        if( isMultipleFile(tableName) ) {
            File file = new File(filename);
            if( !file.exists()) {
                return false;   // key is not exist
            }

            writeJson(filename, row);
        } else {
            File file = new File(filename);
            if( !file.exists()) {
                return false;   // key is not exist
            }

            Map<String, Object> data = readJson(filename);
            if( data == null || data.get(key) == null) {
                return false;   // key is not exist
            }

            data.put(key, row);
            writeJson(filename, data);
        }

        return true;
    }

    @Override
    public boolean delete(String tableName, String keyColumn, String key) throws Exception {
        String filename = this.path + "/" + makeFilename(tableName, key);

//        File folder = new File(this.path + "/" + config.getSource().get("backupFolder"));
//        if( folder.exists() == false ) {
//            folder.mkdir();
//        }

        String backupFilename = this.path + "/" + ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_BACKUP_FOLDER) + "/" + makeBackupFilename(tableName, key);
        File backupFile = new File(backupFilename);
        if( backupFile.exists()) {
            backupFilename += "." + UUID.randomUUID();
        }

        if (isMultipleFile(tableName)) {
            File file = new File(filename);
            if( !file.exists()) {
                return false;
            }

            Map<String, Object> data = readJson(filename);
            if( data == null ) {
                return false;   // key is not exist
            }

            if( !file.delete() ) {
                throw new IOException("Cannot delete file " + filename);
            }
        } else {
            File file = new File(filename);
            if( !file.exists()) {
                return false;   // key is not exist
            }

            Map<String, Object> data = readJson(filename);
            if( data == null || data.get(key) == null) {
                return false;   // key is not exist
            }

            Map<String, Object> row = (Map) data.get(key);
            data.remove(key);
            writeJson(filename, data);
        }

        return true;
    }

    @Override
    public int deleteAll() {
        return 0;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    String getRule(String tableName) {
        return ConfigurationManager.shared().getString("datasource." + this.name + ".filename." + tableName);
    }

    String getBackupRule(String tableName) {
        String rule = "";
        if( tableName.equals(ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_FLOW)) ) {
            rule = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_BACKUP_RULE_FOR_FLOW);
        } else if(tableName.equals(ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_TABLE_FOR_DATASOURCE)) ) {
            rule = ConfigurationManager.shared().getString(FloodgateConstants.META_SOURCE_BACKUP_RULE_FOR_DATASOURCE);
        }

        return rule;
    }


    String makeFilename(String tableName, String key) {
        String rule = getRule(tableName);

        if( rule == null || rule.isEmpty() ) {
            rule = tableName + ".json";
        } else {
            rule = rule.replaceAll("<TABLE>", tableName);
            rule = rule.replaceAll("<ID>", key);
        }

        return rule;
    }

    String makeBackupFilename(String tableName, String key) {
        String rule = getBackupRule(tableName);

        String dateFormat = "yyyy_MM_dd_HH_mm_ss";
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);

        long current = System.currentTimeMillis();
        Date date = new Date(current);

        String currentDateTime = format.format(date);

        if( rule == null || rule.isEmpty() ) {
            rule = currentDateTime + "_" + tableName + ".backup";
        } else {
            rule = rule.replaceAll("<TABLE>", tableName);
            rule = rule.replaceAll("<ID>", key);
            rule = rule.replaceAll("<DATE>", currentDateTime);
        }

        return rule;
    }

    boolean isMultipleFile(String tableName) {
        String rule = getRule(tableName);
        return rule != null && rule.contains("<ID>");
    }

    public Map<String, Object> readJson(String filename) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> map = mapper.readValue(new File(filename), LinkedHashMap.class);
        return map;
    }

    public void writeJson(String filename, Map<String, Object> data) throws Exception {
        try {
            String path = filename.substring(0, filename.lastIndexOf("/"));
            File folder = new File(path);

            if(!folder.exists()) {
                if( !folder.mkdir() ) {
                    throw new IOException("Cannot make folder " + path);
                }
            }

            ObjectMapper mapper = new ObjectMapper();
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(filename), data);
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
