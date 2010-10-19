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
package org.cloudata.core.client.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.cloudata.core.client.cql.statement.QueryStatement;
import org.cloudata.core.common.util.ClassList;


public class StatementFactory {
  public static final String HELP = "HELP";
  public static final String CREATE_TABLE = "CREATE TABLE";
  public static final String SHOW_TABLES = "SHOW TABLES";
  public static final String REPORT_TABLE = "REPORT TABLE";
  public static final String DESC = "DESC";
  public static final String SELECT = "SELECT";
  public static final String DELETE = "DELETE";
  public static final String INSERT = "INSERT";
  public static final String UPLOAD = "UPLOAD INTO";
  public static final String EXIT = "EXIT";
  public static final String DROP_TABLE = "DROP TABLE";
  public static final String TRUNCATE_COLUMN = "TRUNCATE COLUMN";
  public static final String TRUNCATE_TABLE = "TRUNCATE TABLE";
  public static final String RUN_TABLET_ACTION = "RUN ACTION";
  public static final String STOP_TABLET_ACTION = "STOP ACTION";
  public static final String SET_CHARSET = "SET CHARSET";
  public static final String PING_TABLETSERVER = "PING TABLETSERVER";
  public static final String ADD_USER = "ADD USER";
  public static final String SHOW_USER = "SHOW USERS";
  public static final String DELETE_USER = "DELETE USER";
  public static final String MODIFY_USER = "MODIFY USER";
  public static final String START_BALANCER = "START BALANCER";
  
  //Prefix -> Class
  private static Map<String, Class> stmtClasses = new HashMap<String, Class>();
  
  //Prefix -> usage
  public static Map<String, HelpUsage> usages = new HashMap<String, HelpUsage>();

  static {
    List<Class> tStmtClasses = new ArrayList<Class>();
    try {
      List<Class> tmpClasses = ClassList.findClasses(ClassLoader.getSystemClassLoader(), 
          QueryStatement.class, 
          "org.cloudata.core.client.shell", 
          null);
      
      tStmtClasses.addAll(tmpClasses);
      
      tmpClasses = ClassList.findClasses(ClassLoader.getSystemClassLoader(), 
          QueryStatement.class, 
          "org.cloudata.core.client.nql.statement", 
          null);
      
      tStmtClasses.addAll(tmpClasses);
    } catch (Exception e) {
      e.printStackTrace(System.out);
    }
    
    if(tStmtClasses != null) {
      for(Class eachClass: tStmtClasses) {
        QueryStatement statement = null;
        try {
          statement = (QueryStatement)eachClass.newInstance();
        } catch (Exception e) {
          continue;
        }
        if(statement != null) {
          if(statement.getPrefix() != null) {
            stmtClasses.put(statement.getPrefix().toLowerCase(), eachClass);
            usages.put(statement.getPrefix(), statement.getHelpUsage());
          }
        }
      }
    }
  }
  
  public static QueryStatement getStatement(String query) {
    StringTokenizer st = new StringTokenizer(query);
    String preFix = "";
    
    int count = 0;
    while(st.hasMoreTokens()) {
      preFix += (st.nextToken() + " ");
      if(stmtClasses.containsKey(preFix.trim().toLowerCase())) {
        Class stmtClass = stmtClasses.get(preFix.trim().toLowerCase());
        try { 
          QueryStatement statement = (QueryStatement)stmtClass.newInstance();
          if(statement instanceof ShellStatement) {
            ((ShellStatement)statement).setQuery(query);
          }
          return statement;
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
      }
      count++;
      if(count >= 3) {
        //3번째 Token까지만 확인
        break;
      }
    }
    
    return new HelpStatement();
  }

  public static Map<String, HelpUsage> getUsages() {
    return usages;
  }  
}