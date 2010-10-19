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

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Shell;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


public class ShowStatement extends ShellStatement {
  @Override
  public String getPrefix() {
    return StatementFactory.SHOW_TABLES;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    try {
      int tableLength = 0;

      TableSchema[] tables = CTable.listTables(conf);
      tableLength = tables.length;
      if (tableLength == 0) {
        status.setMessage("Table not found.");
        return status;
      }

      Shell.printHead("Table Name");
      for (int i = 0; i < tableLength; i++) {
        //String tableName = tables[i].getTableName().toString();
        Shell.printTable(i, tables[i]);
      }
      Shell.printFoot();

      status.setMessage(tableLength + " table(s) found.");
    } catch (IOException e) {
      status.setException(e);
    }  
    
    return status;
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "List all tables or files.", "SHOW TABLES");
  }
}
