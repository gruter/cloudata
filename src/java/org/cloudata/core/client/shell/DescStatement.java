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

import java.io.IOException;
import java.util.StringTokenizer;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;


public class DescStatement extends ShellStatement {
  @Override
  public String getPrefix() {
    return StatementFactory.DESC;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    StringTokenizer st = new StringTokenizer(query);
    if(st.countTokens() < 2) {
      status.setMessage("Syntax error : Please check 'desc' syntax.");
      return status;
    }
    st.nextToken();
    String tableName = st.nextToken();

    if (tableName == null || tableName.trim().length() == 0) {
      status.setMessage("Syntax error : Please check 'desc' syntax.");
      return status;
    }
  
    try {
      CTable ctable = CTable.openTable(conf, tableName);
      TableSchema tableSchema = ctable.descTable();
      if (tableSchema == null) {
        status.setMessage("Table not found.");
        return status;
      }

      System.out.println("+------+----------------------+");
      System.out.print("| No.  | ");
      System.out.printf("%-20s", "Column Name");
      System.out.println(" |");
      
      ColumnInfo[] columns = tableSchema.getColumnInfoArray();
      for (int i = 0; i < columns.length; i++) {
        System.out.println("+------+----------------------+");
        System.out.print("| ");
        System.out.printf("%-4s", i + 1);
        System.out.print(" | ");
        System.out.printf("%-20s", columns[i]);
        System.out.println(" |");        
      }
      System.out.println("+------+----------------------+");
      System.out.println();

      status.setMessage(columns.length + " column(s) found.");
    } catch (IOException e) {
      status.setException(e);
    }
    
    return status;
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Describe a table infomation.", "DESC <table_name>");
  }
}
