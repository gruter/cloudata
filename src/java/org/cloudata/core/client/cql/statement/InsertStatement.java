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
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.Shell;
import org.cloudata.core.client.cql.element.ColumnElement;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.client.cql.statement.QueryStatement;
import org.cloudata.core.client.shell.HelpUsage;
import org.cloudata.core.client.shell.StatementFactory;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class InsertStatement implements QueryStatement {
  String tableName;
  List<ColumnElement> columns;
  List<String[]> columnValues;
  Row.Key rowKey;
  String rowKeyName;
  
  public void setColumns(List<ColumnElement> columns) {
    this.columns = columns;
  }
  
  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    try {
      if(!rowKeyName.toLowerCase().equals("rowkey")) {
        throw new IOException("Wrong query. insert query must have rowkey in where clause");
      }
      if(!CTable.existsTable(conf, tableName)) {
        throw new IOException("No table [" + tableName + "]");
      }
      
      if(rowKey == null || rowKey.getLength() == 0) {
        throw new IOException("No rowkey field in query.");
      }
      CTable ctable = CTable.openTable(conf, tableName);
      
      List<String> columnNames = new ArrayList<String>();
      if(columns == null || columns.size() == 0) {
        columnNames.addAll(ctable.getTableSchema().getColumns());
      } else {
        for(ColumnElement eachColumn: columns) {
          columnNames.addAll(eachColumn.getColumnNames(ctable));
        }
      }
      
      if(columnNames.size() != columnValues.size()) {
        throw new IOException("Not matched # columns and column values");
      }
      
      Row row = new Row(rowKey);
      for(int i = 0; i < columnNames.size(); i++) {
        String columnName = columnNames.get(i);
        String[] columnValue = columnValues.get(i);
        
        Cell.Key cellKey = (columnValue[0].length() == 0 ? Cell.Key.EMPTY_KEY : new Cell.Key(columnValue[0]));
        byte[] cellValue = (columnValue[1].length() == 0 ? null : columnValue[1].getBytes());
        row.addCell(columnName, new Cell(cellKey, cellValue));
      }
      
      ctable.put(row);
      status.setMessage("1 row inserted.");
    } catch (IOException e) {
      status.setException(e);
    }
    
    return status;
  }

  @Override
  public String getQuery(CloudataConf conf) {
    String result = "INSERT INTO " + tableName + " ";
    
    if(columns != null && columns.size() > 0) {
      String columnQuery = "";
      for(ColumnElement eachColumn: columns) {
        columnQuery += eachColumn.getQuery(conf) + ", ";
      }
      if(columnQuery.length() > 0) {
        columnQuery = columnQuery.substring(0, columnQuery.length() - 2);
      }
      
      result += "(" + columnQuery + ")";
    }
    
    result += " VALUES ";
    
    if(columnValues != null && columnValues.size() > 0) {
      String valueQuery = "";
      
      for(String[] eachValue: columnValues) {
        valueQuery += "('" + eachValue[0] + "','" + eachValue[1] + "'), ";
      }
      if(valueQuery.length() > 0) {
        valueQuery = valueQuery.substring(0, valueQuery.length() - 2);
      }
      
      result += "(" + valueQuery + ")";
    }
    
    result += " WHERE ROWKEY = '" + rowKey.toString() + "'";
    return result;
  }

  public void setRowKeyName(String name) {
    this.rowKeyName = name;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setColumnValues(List<String[]> columnValues) {
    this.columnValues = columnValues;
  }

  public void setRowKey(String rowKey) {
    this.rowKey = new Row.Key(rowKey);
  }
  
  @Override
  public String getPrefix() {
    return StatementFactory.INSERT;
  }
  
  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Insert row into table.",
        "INSERT INTO <table_name> \n" + 
        "       (column_name1[, column_name2, ...]) \n" +
        "VALUES ( ('key1', 'val1')[, ('key1', 'val1'), ...]) \n" +
        " WHERE rowkey='row_key';");
  }
}
