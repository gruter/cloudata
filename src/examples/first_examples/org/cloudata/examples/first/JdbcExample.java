/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudata.examples.first;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;

/**
 * @author jindolk
 *
 */
public class JdbcExample {
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      System.out.println("Usage:java JdbcExample <url> <table_name>");
      System.out.println(" url: jdbc:cloudata://zookeepr_hostname:zookeeper_port:clusterId/cloudata_service_name?user=test&charset=EUC-KR&filesystem=hdfs://hdfshost:50070");
      System.exit(0);
    }
    String url = args[0];
    String tableName = args[1];
    Class.forName("org.cloudata.jdbc.CloudataDriver"); 
    Connection conn = DriverManager.getConnection(url);
    
    Statement stmt = null;
    ResultSet rs = null;
    try {
      boolean exist = false;

      //print table list
      rs = conn.getMetaData().getTables(null, null, null, null);
      System.out.println("--------------- Table -----------------");
      while(rs.next()) {
        System.out.println(rs.getString("Name") + "," + rs.getString("Owner") + "," + rs.getString("Version"));
        if(rs.getString(1).equals(tableName)) {
          exist = true;
        }
      }
      rs.close();
      
      System.out.println("---------------------------------------");
      
      if(exist) {
        System.out.println(tableName + " already exists");
        System.exit(0);
      }
      
      stmt = conn.createStatement();
      
      //create table
      stmt.executeUpdate("create table " + tableName + " (col1, col2) version = 0");
      
      //insert
      insert(tableName, conn);
      
      //select
      select(tableName, conn);
      
      //delete
      delete(tableName, conn);
      
      System.out.println("----------------------------------------------");
      //select
      select(tableName, conn);
    } finally {
      if(rs != null) {
        rs.close();
      }
      if(stmt != null) {
        stmt.close();
      }
      if(conn != null) {
        conn.close();
      }
    }
  }

  private static void delete(String tableName, Connection conn) throws SQLException {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      DecimalFormat df = new DecimalFormat("0000000000");
      String rowKey = df.format(1);
      stmt.executeUpdate("delete col1 from " + tableName + " where rowkey = '" + rowKey + "'");
      
      rowKey = df.format(2);
      stmt.executeUpdate("delete col1 from " + tableName + " where rowkey = '" + rowKey + "' and col1 = 'ck1_2'");

      rowKey = df.format(5);
      stmt.executeUpdate("delete * from " + tableName + " where rowkey = '" + rowKey + "'");
    } finally {
      if(stmt != null) {
        stmt.close();
      }
    }
  }
  
  private static void insert(String tableName, Connection conn) throws SQLException {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      DecimalFormat df = new DecimalFormat("0000000000");
      for(int i = 0; i < 10; i++) {
        String rowKey = df.format(i);
        for(int j = 1; j <= 3; j++) {
          String col1Key = "ck1_" + j;
          String col1Value = "value1_" + j;
  
          String col2Key = "ck2_" + j;
          String col2Value = "value2_" + j;
  
          stmt.executeUpdate("insert into " + tableName + " values ( " +
              "('" + col1Key + "','" + col1Value + "')," +
              "('" + col2Key + "','" + col2Value + "') " +
              ") where rowkey = '" + rowKey + "'");
        }
      }
    } finally {
      if(stmt != null) {
        stmt.close();
      }
    }
  }
  private static void select(String tableName, Connection conn)
      throws SQLException {
    ResultSet rs = null;
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      rs = stmt.executeQuery("select * from " + tableName);
      int count = 0;
      while(rs.next()) {
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
        System.out.print("Row:" + rs.getString(colCount + 1) + ",");
        for(int i = 1; i <= colCount; i++) {
          byte[] value = rs.getBytes(i);
          String valueStr = value == null ? "null" : new String(value);
          System.out.print(rsmd.getColumnName(i) + "[" + rs.getString(i) + ":" + valueStr + ":" + rs.getTimestamp(i) + "]");
          if(i < colCount) {
            System.out.print(", ");
          }
        }
        System.out.print("\n");
        count++;
      }
      System.out.println(count + " values selected");
    } finally {
      if(rs != null) {
        rs.close();
      }
      if(stmt != null) {
        stmt.close();
      }
    }
  }
}
 