package com.dkl.blog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

/**
 * Created by dongkelun on 2021/5/7 10:06
 *
 * java 访问 kerberos认证下的 Hive Server和 Spark Thrift Server
 */
public class SparkThriftServerDemoWithKerberos {
    private static String HIVE_JDBC_URL = "jdbc:hive2://192.168.44.128:10000/sjtt;principal=hive/indata-192-168-44-128.indata.com@INDATA.COM";
    private static final String SPARK_JDBC_URL = "jdbc:hive2://192.168.44.128:20003/sjtt;" +
            "principal=HTTP/indata-192-168-44-128.indata.com@INDATA.COM?" +
            "hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice;";
    private static final String PRINCIPAL = "hive/indata-192-168-44-128.indata.com@INDATA.COM";
    private static final String KEYTAB = "D:\\conf\\inspur\\hive.service.keytab";
    private static final String KRB5 = "D:\\conf\\inspur\\krb5.conf";

    private static Configuration conf = null;

    static {
        conf = new Configuration();
    }

    public static void main(String[] args) throws SQLException {
        loadConfiguration();
        //----------------------------------connect hive----------------------------------//
        System.out.println("select from hive");
        jdbcDemo(HIVE_JDBC_URL);

        //------------------------------connect spark thrift server-----------------------//
        System.out.println("select from spark thrift server");
        jdbcDemo(SPARK_JDBC_URL);
    }

    public static void jdbcDemo(String jdbc_url) throws SQLException {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbc_url);
            selectTable(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    public static void selectTable(Connection connection) {
        String sql = "select * from trafficbase_cljbxx limit 10";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(sql);
            System.out.println("=====================================");
            while (rs.next()) {
                System.out.println(rs.getString(1) + "," + rs.getString(2));
            }
            System.out.println("=====================================");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(stmt);
            close(rs);
        }

    }

    private static void loadConfiguration() {
        // 初始化配置文件
        try {
            conf.set("hadoop.security.authentication", "kerberos");
            System.setProperty("java.security.krb5.conf", KRB5);// krb5文件路径
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(PRINCIPAL, KEYTAB);// 入参：principal、keytab文件
        } catch (IOException ioE) {
            System.err.println("使用keytab登陆失败");
            ioE.printStackTrace();
        }

    }

    /**
     * 关闭Statement
     *
     * @param stmt
     */
    private static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭ResultSet
     *
     * @param rs
     */
    private static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
