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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Shell;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.common.conf.CloudataConf;


public class TruncateStatement extends ShellStatement {
  private String tableName;
  private String columnName;
  
  private boolean addToken(int pos, String token) {
    switch(pos) {
    case 2:
      columnName = token;
      break;
    case 3:
      if(!"FROM".equals(token.toUpperCase())) {
        return false;
      }
      break;
    case 4:
      tableName = token;
      break;
    } 
    
    return true;
  }
  
  @Override
  public String getPrefix() {
    return StatementFactory.TRUNCATE_COLUMN;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    byte[] queryBytes = query.getBytes();
    int pos = 0;
    int writeBytes = 0;
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream(256);
    
    for(int i = 0; i < queryBytes.length; i++) {
      if(queryBytes[i] == ' ' || queryBytes[i] == '\n') {
        if(writeBytes > 0) {
          if(!addToken(pos, bout.toString())) {
            status.setMessage("Syntax error : Please check 'truncate column' syntax.");
            return status;
          }
          writeBytes = 0;
          bout.reset();
          pos++;
        }
      } else {
        bout.write(queryBytes[i]);
        writeBytes++;
      }
    }
    if(writeBytes > 0) {
      if(!addToken(pos, bout.toString())) {
        status.setMessage("Syntax error : Please check 'truncate column' syntax.");
        return status;
      }
      writeBytes = 0;
      bout.reset();
    }
    
    if (this.tableName == null) {
      status.setMessage("Syntax error : Please check 'truncate column' syntax.");
      return status;
    }
    
    try {
      CTable ctable = CTable.openTable(Shell.conf, this.tableName);
      ctable.truncateColumn(columnName);
      
      status.setMessage("Column truncate successfully.");
    } catch (IOException e) {
      status.setException(e);
    }
    
    return status;
  }
  
  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "truncate a column in a table. all data in column will be removed",
        "TRUNCATE COLUMN <column name> \n" +
        "  FROM <table_name>;");
  }
}
