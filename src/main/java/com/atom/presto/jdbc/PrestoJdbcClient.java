package com.atom.presto.jdbc;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoStatement;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

/**
 * @author Atom
 */
public class PrestoJdbcClient {

    public static void printRow(ResultSet rs, int[] types) throws SQLException {
        for (int i = 0; i < types.length; i++) {
            System.out.println(" ");
            System.out.println(rs.getObject(i + 1));
        }
    }

    public static void connect() throws SQLException {
        //设置时区
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        PrestoConnection conn = null;
        try {
            //jdbc:presto://host:port/catalog/schema;用户名随意指定即可
            conn = (PrestoConnection) DriverManager.getConnection("jdbc:presto://10.16.118.234:18080/hive/default", "xxx", null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        PrestoStatement statement = null;
        try {
            statement = (PrestoStatement) conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //获取表的colums信息
//        String query = "show columns from liuyaming_table001";
        String query = "select * from liuyaming_table001";
        ResultSet rs = null;

        try {
            rs = statement.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        int cn = rs.getMetaData().getColumnCount();
        int[] types = new int[cn];
        for (int i = 1; i <= cn; i++) {
            types[i - 1] = rs.getMetaData().getColumnType(i);
        }

        try {
            while (rs.next()) {
                printRow(rs, types);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        rs.close();
        conn.close();

    }

    public static void main(String[] args) throws SQLException {
        connect();
    }


}
