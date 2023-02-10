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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class DBUtils {

    public static String makeURL(String dbType, String ipport, String sid) {
        String[] ipports = ipport.split(",");

        return makeURL(dbType, ipports, sid);
    }

    public static String makeURL(String dbType, String[] ipports, String sid) {
        switch(dbType.trim().toUpperCase()) {
            case "ORACLE": {
                if( ipports.length == 1 ) {
                    if( sid.startsWith("/") ) { // for ServiceName
                        return "jdbc:oracle:thin:@" + ipports[0].trim() + sid.trim();
                    }
                    return "jdbc:oracle:thin:@" + ipports[0].trim() + ":" + sid.trim();
                } else {
                    String url = "jdbc:oracle:thin:@(DESCRIPTION=(FAILOVER=ON)(LOAD_BALANCE=OFF)(ADDRESS_LIST=";
                    for (int i = 0; i < ipports.length; i++) {
                        String[] ip_port = ipports[i].split(":");
                        url += "(ADDRESS=(PROTOCOL=TCP)(HOST=" + ip_port[0] + ")(PORT=" + ip_port[1] + "))";
                    }

                    sid = sid.replace("/", "").trim();
                    return url += ")(CONNECT_DATA=(SERVICE_NAME=" + sid + ")(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=25)(DELAY=10))))";
                }
            }
            case "TIBERO": {
                if (ipports.length == 1 ) {
                    if (sid.startsWith("/") ) { // for ServiceName
                        return "jdbc:tibero:thin:@" + ipports[0].trim() + sid.trim();
                    }
                    return "jdbc:tibero:thin:@" + ipports[0].trim() + ":" + sid.trim();
                } else {
                    String url = "jdbc:tibero:thin:@(DESCRIPTION=(FAILOVER=ON)(LOAD_BALANCE=OFF)(ADDRESS_LIST=";
                    for (int i = 0; i < ipports.length; i++) {
                        String[] ip_port = ipports[i].split(":");
                        url += "(ADDRESS=(PROTOCOL=TCP)(HOST=" + ip_port[0] + ")(PORT=" + ip_port[1] + "))";
                    }
                     sid = sid.replace("/", "").trim();
                     return url + ")(CONNECT_DATA=(SERVICE_NAME=" + sid + ")(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=25)(DELAY=10))))";
                     }
            }
            case "POSTGRESQL":
                return "jdbc:postgresql://" + ipports[0] + "/" + sid;
            case "GREENPLUM":
                return "jdbc:postgresql://" + ipports[0] + "/" + sid;
            case "MSSQL":
                return "jdbc:sqlserver://" + ipports[0] + ";databaseName=" + sid;
            case "MYSQL_OLD":
                return "jdbc:mysql://" + ipports[0] + "/" + sid + "?characterEncoding=UTF-8&useConfigs=maxPerformance";
            case "MYSQL":
                return "jdbc:mysql://" + ipports[0] + "/" + sid + "?serverTimezone=UTC&characterEncoding=UTF-8&useConfigs=maxPerformance";

            case "MARIADB":
                return "jdbc:mysql://" + ipports[0] + "/" + sid + "?characterEncoding=UTF-8&useConfigs=maxPerformance";
            case "DB2":
                return "jdbc:db2://" + ipports[0] + "/" + sid;
            default:
                return "";
        }
    }

    public static String connect(String url, String userid, String passwd) throws Exception {
        try ( Connection con = DriverManager.getConnection(url, userid, passwd) ) {
            DatabaseMetaData databaseMetaData = con.getMetaData();

            // return String.format("JDBC Version : %d.%d", databaseMetaData.getJDBCMajorVersion(), databaseMetaData.getJDBCMinorVersion());
            return String.format("%s : %s", databaseMetaData.getDatabaseProductName(), databaseMetaData.getDatabaseProductVersion());
        } catch(Exception e) {
            throw e;
        }
    }

    public static boolean checkTable(String url, String userid, String passwd, String table) throws Exception {
        try ( Connection con = DriverManager.getConnection(url, userid, passwd) ) {
            DatabaseMetaData databaseMetaData = con.getMetaData();

            ResultSet resultSet = databaseMetaData.getTables(null, null, tatble, new String[] {"TABLE"});

            return resultSet.next();
        } catch(Exception e) {
            throw e;
        }
    }
}
