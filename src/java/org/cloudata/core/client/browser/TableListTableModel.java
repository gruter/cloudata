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
package org.cloudata.core.client.browser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Shell;
import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.tablet.TableSchema;


@SuppressWarnings("serial")
public class TableListTableModel extends DefaultTableModel {
  private String[] columnNames = {"Table name", "Owner", "Versions", "Desc"};

  private Vector<TableSchema> tableSchemas = new Vector<TableSchema>();
  
  public TableListTableModel() {
    
  }

  @Override
  public int getColumnCount() {
    return columnNames.length;
  }

  @Override
  public String getColumnName(int column) {
    return columnNames[column];
  }

  @Override
  public int getRowCount() {
    if(tableSchemas == null) {
      return 0;
    }
    return tableSchemas.size();
  }

  @Override
  public Object getValueAt(int row, int column) {
    TableSchema tableSchema = tableSchemas.get(row);
    if(column == 0) {
      return tableSchema.getTableName();
    } else if(column == 1) {
      return tableSchema.getOwner();
    } else if(column == 2) {
      return tableSchema.getNumOfVersion();
    } else if(column == 3) {
      return tableSchema.getDescription();
    } else {
      return "";
    }
  }

  public void loadTableSchema() {
    try {
      tableSchemas.clear();
      TableSchema[] tables = CTable.listTables(Shell.conf);
      if(tables == null) {
        return;
      }

      Arrays.sort(tables);
      
      for(TableSchema eachTableInfo:tables) {
        tableSchemas.add(eachTableInfo);
      }
      this.fireTableDataChanged();
    } catch (IOException e) {
      JOptionPane.showMessageDialog(CloudataBrowser.frame, StringUtils.stringifyException(e), "Error",
          JOptionPane.ERROR_MESSAGE);
    }
  }
  
  @Override
  public void setValueAt(Object obj, int row, int column) {
    
  }

  public TableSchema getTableInfo(int minSelectionIndex) {
    if(minSelectionIndex < 0) {
      return null;
    }
    return tableSchemas.get(minSelectionIndex);
  }
}
