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
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Vector;

import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Shell;
import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;


@SuppressWarnings("serial")
public class TabletStatusTableModel extends DefaultTableModel {
  private String[] columnNames = {"Tablet name", "hostName", "end rowkey", "summary"};

  private Vector<TabletReport> tabletReports = new Vector<TabletReport>();
  
  public TabletStatusTableModel() {
    
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
    if(tabletReports == null) {
      return 0;
    }
    return tabletReports.size();
  }

  @Override
  public Object getValueAt(int row, int column) {
    TabletReport tabletReport = tabletReports.get(row);
    if(tabletReport == null) {
      return "Report error";
    }
    
    if(column == 0) {
      return tabletReport.getTabletInfo().getTabletName();
    } else if(column == 1) {
      return tabletReport.getTabletInfo().getAssignedHostName();
    } else if(column == 2) {
      return tabletReport.getTabletInfo().getEndRowKey();
    } else if(column == 3) {
      StringWriter writer = new StringWriter();
      try {
        tabletReport.print(writer);
        return writer.getBuffer().toString().replace("<br>", "\n");
      } catch (IOException e) {
        return e.getMessage();
      }
    } else {
      return "";
    }
  }

  public void initTabletReport(String tableName) {
    try {
      tabletReports.clear();
      
      CTable ctable = CTable.openTable(Shell.conf, tableName);
      TabletInfo[] listTabletInfos = ctable.listTabletInfos();
      if(listTabletInfos == null)   return;
      
      for(int i = 0; i < listTabletInfos.length; i++) {
        TabletReport tabletReport = ctable.getTabletReport(listTabletInfos[i]);
        if(tabletReport != null) {
          tabletReports.add(tabletReport);
        } else {
          tabletReports.add(null);
        }
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
}
