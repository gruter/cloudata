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
package org.cloudata.core.client.cql.statement;

import java.io.IOException;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.shell.HelpUsage;
import org.cloudata.core.client.shell.StatementFactory;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class DropStatement implements QueryStatement {
  private String tableName;
  
  @Override
  public String getQuery(CloudataConf conf) {
    return "DROP TABLE " + tableName; 
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    try {
      if(tableName.equals(Constants.TABLE_NAME_ROOT) || tableName.equals(Constants.TABLE_NAME_META)) {
        new IOException("ROOT or META table are system table. Can't drop.");
      }      
      CTable.dropTable(conf, tableName);
      
      status.setMessage(tableName + " table droped.");
    } catch (IOException e) {
      status.setException(e);
    }
    return status;    
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Drop table.", "DROP TABLE <table_name>");
  }

  @Override
  public String getPrefix() {
    return StatementFactory.DROP_TABLE;
  }

}
