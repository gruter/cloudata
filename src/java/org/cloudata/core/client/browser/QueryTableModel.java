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

import java.awt.GridLayout;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableModel;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.procedure.Selector;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.util.StringUtils;


@SuppressWarnings("serial")
public class QueryTableModel extends DefaultTableModel {
  String[] columnNames;

  Vector<Vector> rows = new Vector<Vector>();

  String gsLapTime = "";

  Vector columnLength = new Vector();

  TableScanner scanner;
  
  Selector selector;
  
  Row.Key previousRowKey;
  
  int numRows;
  
  boolean moreData = false;;
  
  int maxRows;
  
  int currentPage;
  
  public boolean hasMoreData() {
    return moreData;
  }
  
  public int getNumRows() {
    return numRows;
  }
  
  public boolean executeQuery(String query, int maxRows, boolean next) {
    try {
      if (!next) {
        this.maxRows = maxRows;
        close();
        rows.clear();
        selector = new Selector(query);
      }
      long lBeforeTime = System.currentTimeMillis();
      
      scanner = selector.getScanner();

      long lAfterTime = System.currentTimeMillis();
      // Lap Time ???
      calLapTime(lBeforeTime, lAfterTime);

      String[] queryColumnNames = selector.getColumns(); 
      columnNames = new String[queryColumnNames.length + 1];
      columnNames[0] = "Row.Key";
      System.arraycopy(queryColumnNames, 0, columnNames, 1, queryColumnNames.length);
      
      setTableData();
      return true;
    } catch (Exception e) {
      JOptionPane.showMessageDialog(CloudataBrowser.frame, StringUtils.stringifyException(e), "Error",
          JOptionPane.ERROR_MESSAGE);
      
      return false;
    }
  }

  private void setTableData() {
    try {
      int count = 0;
      Row dataRow = null;
      
      int skipCount = 0;
      
      while ( (dataRow = scanner.nextRow()) != null ) {
        skipCount++;
        if(skipCount < currentPage * maxRows) {
          continue;
        }
        
        boolean first = true;
        
        Vector<String> row = new Vector<String>();
        
        if(first) {
          row.add(dataRow.getKey().toString());
          first = false;
        }
        
        List[] cells = new List[columnNames.length - 1];
        
        int maxCount = 0;
        for(int i = 1; i < columnNames.length; i++) {
          cells[i - 1] = dataRow.getCellList(columnNames[i]);
          if(cells[i - 1] != null && cells[i - 1].size() > maxCount) {
            maxCount = cells[i - 1].size();
          }
        }
        
        for(int i = 0; i < maxCount; i++) {
          for(int j = 0; j < cells.length; j++) {
            if(cells[j] != null && cells[j].size() > i) {
              Cell cell = (Cell)cells[j].get(i);
              row.add(cell.getPrintValue());
            } else {
              row.add("");
            }
          }
          rows.add(row);
          row = new Vector<String>();
          //empty rowkey
          row.add("");
        }
        
        numRows++;
        count++;
        if (count >= maxRows) {
          break;
        }
      }
      if (count == 0) {
        JOptionPane.showMessageDialog(CloudataBrowser.frame, "No more data.", "Info",
            JOptionPane.INFORMATION_MESSAGE);
      }
      
      if(dataRow != null) {
        moreData = true;
      }
      fireTableChanged(null);
    } catch (IOException e) {
      JOptionPane.showMessageDialog(CloudataBrowser.frame, StringUtils.stringifyException(e), "Error", JOptionPane.ERROR_MESSAGE);
      close();
    } finally {
      try {
        if(scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      scanner = null;
      currentPage++;
    }
  }

  private void close() {
    if(scanner != null) {
      try {
        scanner.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      scanner = null;
    }
    previousRowKey = null;
    numRows = 0;
    moreData = false;
    currentPage = 0;
  }

  public String getLapTime() {
    return gsLapTime;
  }

  public void calLapTime(long lBeforeTime, long lAfterTime) {
    double lap = lAfterTime - lBeforeTime;
    Double laptime = new Double(lap / 1000.0);

    long min = 0;
    if (laptime.doubleValue() > 60) {
      min = (long) (laptime.doubleValue() / 60);
    }

    if (min == 0) {
      gsLapTime = laptime.toString() + " sec";
    } else {
      gsLapTime = padZero("" + min, 2) + " min " + laptime.toString() + " sec";
    }
  }

  public String padZero(String source, int strLength) {
    if (source == null)
      source = "";
    int len = source.getBytes().length;
    String sResult = "";

    if (len >= strLength)
      return source.substring(0, strLength);

    for (int i = 0; i < strLength - len; i++) {
      sResult = sResult + "0";
    }
    return sResult + source;
  }

  public void saveToFile(String sFileName) {
    int rowCount = getRowCount();
    int colCount = getColumnCount();

    if (rowCount == 0) {
      JOptionPane.showMessageDialog(CloudataBrowser.frame, "No Data", "Warn", JOptionPane.ERROR_MESSAGE);
      return;
    }

    JTextField fdSplitChar = new JTextField(3);
    JCheckBox ckFull = new JCheckBox();
    JPanel p = new JPanel(new GridLayout(2, 2));
    p.add(new JLabel("구분문자: "));
    p.add(fdSplitChar);
    p.add(new JLabel("컬럼길이: "));
    p.add(ckFull);

    boolean isFull = false;
    String sSplitChar = "\t";

    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new FileWriter(sFileName, false));

      for (int i = 0; i < rowCount; i++) {
        StringBuffer sLine = new StringBuffer();
        ;
        for (int j = 0; j < colCount; j++) {
          Object value = getValueAt(i, j);
          if (value == null) {
            value = " ";
          }

          if (isFull == true) {
            int length = ((Integer) columnLength.elementAt(j)).intValue();
            sLine.append(padString(value.toString(), " ", length));
          } else {
            sLine.append(value.toString());
          }

          if (j < colCount - 1) {
            sLine.append(sSplitChar);
          }
        }

        out.write(sLine.toString());
        out.newLine();
      }
      JOptionPane.showMessageDialog(CloudataBrowser.frame, "저장되었습니다.", "완료", 
          JOptionPane.INFORMATION_MESSAGE);
    } catch (Exception e) {
      JOptionPane.showMessageDialog(CloudataBrowser.frame, e.toString(), "파일저장오류",
          JOptionPane.INFORMATION_MESSAGE);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (Exception err) {
        }
      }
    }
  }

  public String padString(String source, String padChar, int strLength) {
    if (source == null)
      source = "";

    int len = source.getBytes().length;
    String sResult = source;

    if (len >= strLength)
      return source.substring(0, strLength);

    for (int i = 0; i < strLength - len; i++) {
      sResult = sResult + padChar;
    }
    return sResult;
  }

  // ///////////////////////////////////////////////////////////////
  //
  // TableModel interface ????
  //
  // ///////////////////////////////////////////////////////////////
  public String getColumnName(int column) {
    if(column == 0) {
      return "No";
    }
    return columnNames[column - 1];
  }

  public int getColumnCount() {
    if(columnNames == null) {
      return 0;
    }
    return columnNames.length + 1;
  }

  public int getRowCount() {
    if (rows == null)
      return 0;
    else
      return rows.size();
  }

  public Object getValueAt(int row, int column) {
    if(column == 0) {
      return row + 1;
    } else {
      Vector rowDatas = rows.get(row);
      
      return rowDatas.get(column - 1);
    }
  }

  public void setValueAt(Object value, int row, int column) {
//    Vector dataRow = (Vector) rows.elementAt(row);
//    dataRow.setElementAt(value, column);
  }

  public boolean isCellEditable(int row, int column) {
    return true;
  }

  public void clear() {
    close();
    if(rows != null) {
      rows.clear();
      rows = null;
    }
  }
}
