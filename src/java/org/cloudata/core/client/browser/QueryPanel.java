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

////////////////////////////////////////////////////////////////////////////////////////////////////////////  
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;

@SuppressWarnings("serial")
class QueryPanel extends JPanel {
  private JTable queryTable;

  private QueryTableModel queryTableModel;

  private JTextArea queryEditor;

  private int bufferPosition = 0;

  private JLabel timeLabel = null;

  private JButton nextDataButton;
  
  private JButton closeButton;
  
  private JTextField fetchField;
  
  private QueryPanelModel queryPanelModel = new QueryPanelModel();
  
  public QueryPanel() {
    super();
    CloudataBrowser.setTheme();

    queryEditor = new JTextArea();
    Font font = new Font("dialoginput", Font.PLAIN, 12);
    queryEditor.setFont(font);
    queryEditor.setTabSize(2);
    queryEditor.setLineWrap(true);

    queryEditor.addKeyListener(new KeyAdapter() {
      public void keyPressed(KeyEvent e) {
        int keyCode = e.getKeyCode();
        if (keyCode == 112) {
          runQuery();
        }
      }
    });

    JScrollPane editorPane = new JScrollPane(queryEditor);

    queryTableModel = new QueryTableModel();
    queryTable = new JTable(queryTableModel);
    queryTable.setAutoscrolls(false);
    queryTable.setCellSelectionEnabled(true);
    new ExcelAdapter(queryTable);

    queryTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    JScrollPane tablePane = new JScrollPane(queryTable);

    editorPane.setMinimumSize(new Dimension(0, 0));
    tablePane.setMinimumSize(new Dimension(0, 0));
    JSplitPane splitpane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, true,
        editorPane, tablePane);
    splitpane.setDividerLocation(100);
    splitpane.setOneTouchExpandable(true);

    JButton runButton = new JButton("Run(F1)");
    runButton.setToolTipText("Run Query");

    nextDataButton = new JButton("next data");
    nextDataButton.setEnabled(false);

    JButton previousButton = new JButton("<query");
    previousButton.setToolTipText("show pewvious query");

    JButton nextButton = new JButton("query>");
    nextButton.setToolTipText("show next query");

    JButton lastButton = new JButton("query>>");
    nextButton.setToolTipText("show last query");

    closeButton = new JButton("Close");
    closeButton.setToolTipText("Close selected query tab");
    closeButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        CloudataBrowser.getInstance().removeQueryTab();
      }
    });

    JPanel timePanel = new JPanel();
    timeLabel = new JLabel("");
    fetchField = new JTextField("50", 5);
    timePanel.add(new JLabel("Max rows:"));
    timePanel.add(fetchField);
    timePanel.add(timeLabel);

    JPanel buttonPanel = new JPanel();
    //buttonPanel.setLayout(new GridLayout(1, 5));
    buttonPanel.setLayout(new FlowLayout(FlowLayout.LEFT));

    runButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        runQuery();
      }
    });

    previousButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if (bufferPosition > 0) {
          queryEditor.setText(queryPanelModel.getQuery(bufferPosition - 1));
          bufferPosition = bufferPosition - 1;
        }
      }
    });

    nextButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if (bufferPosition < (queryPanelModel.getQuerySize() - 1)) {
          bufferPosition = bufferPosition + 1;
          queryEditor.setText(queryPanelModel.getQuery(bufferPosition));
        }
      }
    });
    
    lastButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if (queryPanelModel.getQuerySize() > 0) {
          bufferPosition = queryPanelModel.getQuerySize() - 1;
          queryEditor.setText(queryPanelModel.getQuery(bufferPosition));
        }
      }
    });

    nextDataButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        queryTableModel.executeQuery(queryEditor.getText(), 0, true);
        String message = "Query result:" + queryTableModel.getNumRows() + " rows retrived.";
        if(queryTableModel.hasMoreData()) {
          message += " click next data button for more data";
          nextDataButton.setEnabled(true);
        } else {
          nextDataButton.setEnabled(false);
        }
        CloudataBrowser.getInstance().setStatusBar(message);      
      }
    });

    buttonPanel.add(runButton);
    buttonPanel.add(nextDataButton);
    buttonPanel.add(previousButton);
    buttonPanel.add(nextButton);
    buttonPanel.add(lastButton);
    buttonPanel.add(closeButton);
//    buttonPanel.add(excelButton);
//    buttonPanel.add(clearButton);
    buttonPanel.add(timePanel);

    this.setBackground(Color.white);
    this.setLayout(new BorderLayout());
    this.add(buttonPanel, "North");
    this.add(splitpane, "Center");
  }

  public void setCloseButtonEnable(boolean enable) {
    closeButton.setEnabled(enable);
  }
  
  public void closePanel() {
    queryTableModel.clear();
    queryPanelModel.clear();
  }
  
  private void runQuery() {
    String sInputText = queryEditor.getText();
    if (sInputText != null && sInputText.trim().length() > 0) {
      bufferPosition = 0;
      queryPanelModel.addQuery(sInputText);
      bufferPosition = queryPanelModel.getQuerySize() - 1;

      CloudataBrowser.getInstance().setStatusBar("executing query");
      boolean success = queryTableModel.executeQuery(sInputText, Integer.parseInt(fetchField.getText()), false);
      if(success) {
        queryPanelModel.saveQuery(sInputText);
      }
      timeLabel.setText(queryTableModel.getLapTime());
      queryTable.getSelectionModel().setSelectionInterval(0, 0);
      
      String message = "Query result:" + queryTableModel.getNumRows() + " rows retrived.";
      if(queryTableModel.hasMoreData()) {
        message += " click next data button for more data";
        nextDataButton.setEnabled(true);
      } else {
        nextDataButton.setEnabled(false);
      }
      CloudataBrowser.getInstance().setStatusBar(message);
    }
  }

  public void setQuery(String sQuery) {
    queryEditor.setText(sQuery);
  }
}