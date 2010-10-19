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

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.tablet.TableSchema;


@SuppressWarnings("serial")
public class TableSchemaPanel extends JPanel {
  JTable tableListTable;

  TableListTableModel tableListTableModel;

  JTextArea tableSchemaTextArea;

  JTable tabletStatusTable;
  
  TabletStatusTableModel tabletStatusTableModel;
  
  String selectedTableName;
  
  public TableSchemaPanel() {
    super();
    CloudataBrowser.setTheme();

    tableSchemaTextArea = new JTextArea();
    Font font = new Font("dialoginput", Font.PLAIN, 12);
    tableSchemaTextArea.setFont(font);
    tableSchemaTextArea.setTabSize(2);
    tableSchemaTextArea.setLineWrap(true);

    JScrollPane tableSchemaTextAreaPane = new JScrollPane(tableSchemaTextArea);
    tableSchemaTextAreaPane.setBorder(BorderFactory.createTitledBorder("Schema"));
    
    tableListTableModel = new TableListTableModel();
    tableListTable = new JTable(tableListTableModel);
    tableListTable.setAutoscrolls(false);
    tableListTable.setRowSelectionAllowed(true);
    ListSelectionModel rowSm = tableListTable.getSelectionModel();
    rowSm.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    rowSm.addListSelectionListener(new ListSelectionListener() {
      public void valueChanged(ListSelectionEvent e) {
        ListSelectionModel lsm = (ListSelectionModel) e.getSource();
        if (lsm.isSelectionEmpty()) {
          // System.out.println("Debug> No rows are selected.");
        } else {
          TableSchema tableSchema = tableListTableModel.getTableInfo(lsm.getMinSelectionIndex());
          selectedTableName = tableSchema.getTableName();
          showSchema(tableSchema);
        }
      }
    });
    
    new ExcelAdapter(tableListTable);
    
    tabletStatusTableModel = new TabletStatusTableModel();
    tabletStatusTable = new JTable(tabletStatusTableModel);
    tabletStatusTable.setAutoscrolls(false);
    tabletStatusTable.setRowSelectionAllowed(true);
    new ExcelAdapter(tabletStatusTable);

    JScrollPane tabletStatusTablePane = new JScrollPane(tabletStatusTable);
    tabletStatusTablePane.setBorder(BorderFactory.createTitledBorder("Tablet Status"));
    
    JSplitPane rightPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, true,
        tableSchemaTextAreaPane, tabletStatusTablePane);
    rightPane.setDividerLocation(200);
    rightPane.setOneTouchExpandable(true);
    
    JSplitPane splitpane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, true,
        new JScrollPane(tableListTable), rightPane);
    splitpane.setDividerLocation(300);
    splitpane.setOneTouchExpandable(true);

    JPanel buttonPanel = new JPanel();
    // buttonPanel.setLayout(new GridLayout(1, 5));
    buttonPanel.setLayout(new FlowLayout(FlowLayout.LEFT));

    JButton refreshButton = new JButton("Refresh");
    refreshButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        tableListTableModel.loadTableSchema();
      }
    });

    JButton tabletButton = new JButton("Get Tablet Info");
    tabletButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if(selectedTableName != null) {
          CloudataBrowser.getInstance().setStatusBar("fetching Tablet Info [" + selectedTableName + "]");
          tabletStatusTableModel.initTabletReport(selectedTableName);
          CloudataBrowser.getInstance().setStatusBar("completed fetching Tablet Info [" + selectedTableName + "]");
        } else {
          JOptionPane.showMessageDialog(CloudataBrowser.frame, "Select table", "Info",
              JOptionPane.INFORMATION_MESSAGE);
        }
      }
    });

    buttonPanel.add(refreshButton);
    buttonPanel.add(tabletButton);

    this.setBackground(Color.white);
    this.setLayout(new BorderLayout());
    this.add(buttonPanel, "North");
    this.add(splitpane, "Center");

    tableListTableModel.loadTableSchema();
  }
  
  private void showSchema(TableSchema tableSchema) {
    tableSchemaTextArea.setText("");
    
    String result = "";
    for(String eachColumn: tableSchema.getColumns()) {
      result += eachColumn + "\n";
    }
    tableSchemaTextArea.setText(result);
  }

  public void reload() {
    tableListTableModel.loadTableSchema();
  }
}
