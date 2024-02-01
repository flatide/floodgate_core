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

package com.flatide.floodgate.agent.connector;

import java.util.Map;

public class ConnectorFactory {
    //Logger logger = LogManager.getLogger(ConnectorFactory.class);

    private static final ConnectorFactory instance = new ConnectorFactory();

    public static ConnectorFactory shared() {
        return instance;
    }

    private ConnectorFactory() {
    }

    public ConnectorBase getConnector(Map<String, Object> info) throws Exception {
        ConnectorBase con;

        String method = (String) info.get(ConnectorTag.CONNECTOR.name());

        switch( method ) {
            case "JDBC":
                String dbType = (String) info.get(ConnectorTag.DBTYPE.name());
                loadDriver(dbType);
                con = new ConnectorDB();
                break;
            case "FILE":
                con = new ConnectorFile();
                break;
            case "FTP":
                con = new ConnectorFTP();
                break;
            case "SFTP":
                con = new ConnectorSFTP();
                break;
            default:
                return null;
        }

        return con;
    }

    private void loadDriver(String dbType) throws Exception {
        switch(dbType.trim().toLowerCase()) {
            case "oracle":
                Class.forName("oracle.jdbc.driver.OracleDriver");
                break;
            case "mysql":
                Class.forName("com.mysql.cj.jdbc.Driver");
                break;
            case "mysql_old":
                Class.forName("com.mysql.jdbc.Driver");
                break;
            case "mariadb":
                Class.forName("com.mysql.jdbc.Driver");
                break;
            case "postgresql":
                Class.forName("org.postgresql.Driver");
                break;
            case "greenplum":
                Class.forName("org.postgresql.Driver");
                break;
            case "mssql":
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                break;
            case "db2":
                Class.forName("com.ibm.db2.jcc.DB2Driver");
                break;
            case "tibero":
                Class.forName("com.tmax.tibero.jdbc.TbDriver");
                break;
            default:
                break;
        }
    }

}
