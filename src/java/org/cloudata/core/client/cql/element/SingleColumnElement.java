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
package org.cloudata.core.client.cql.element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.CTable;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class SingleColumnElement implements ColumnElement {
  private String columnName;
  private boolean timestamp; 
  
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
  
  public void setTimestamp(boolean timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String getQuery(CloudataConf conf) {
    if(timestamp) {
      return columnName + ".TIMESTAMP"; 
    } else {
      return columnName;
    }
  }

  @Override
  public List<String> getColumnNames(CTable ctable) throws IOException {
    List<String> columns = new ArrayList<String>();
    columns.add(columnName);
    return columns;
  }
  
  public boolean isTimestamp() {
    return timestamp;
  }

  public String getColumnName() {
    return this.columnName;
  }
  
  public boolean isRowKey() {
    return columnName.toLowerCase().equals("rowkey");
  }
}
