package com.dkl.blog;

import java.sql.*;

/**
 * Created by dongkelun on 2021/2/5 17:07
 */
public class SparkThriftServerDemo {
    private static String HIVE_JDBC_URL = "jdbc:hive2://192.168.44.128:10000/default";
    private static final String SPARK_JDBC_URL = "jdbc:hive2://192.168.44.128:10001/default?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice";


    public static void main(String[] args) throws SQLException {
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
        String sql = "select * from test limit 10";
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
