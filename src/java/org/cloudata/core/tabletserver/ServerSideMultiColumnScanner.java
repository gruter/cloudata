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
package org.cloudata.core.tabletserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.LeaseHolder;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;


/**
 * TabletServer에서 gets() 처리를 위해 사용하는 scanner
 * TabletServer에서 scan 처리를 위해서 기본적으로는 Tablet의 scanner를 이용하지만 
 * 여러 column, row에 대한 처리 부분이 복잡하여 별도의 클래스로 구성 
 * @author 김형준
 *
 */
class ServerSideMultiColumnScanner implements Constants {
  public static final Log LOG = LogFactory.getLog(ServerSideMultiColumnScanner.class.getName());
  
  private String[] columnNames;
  
  private TabletScanner[] scanners;

  private SortedSet<ColumnValueItem> columnValueBuf = new TreeSet<ColumnValueItem>();
  
//  private boolean first = true;
  
  private Row.Key previousRowKey;
  
  private List<ColumnValue>[] columnValuesForRowScan;
  
  private Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
  
  private Map<String, Integer> columnMap = new HashMap<String, Integer>(); 
  
  private Map<Integer, List<ColumnValue>> columnScanBuf = new HashMap<Integer, List<ColumnValue>>();
  
  private TabletServer tabletServer;
  
  private int numScannedColumnValue;
  
  @SuppressWarnings("unchecked")
  protected ServerSideMultiColumnScanner(TabletServer tabletServer, RowFilter rowFilter, TabletScanner[] scanners) throws IOException {
    this.columnNames = rowFilter.getColumns();
    this.scanners = scanners;
    this.tabletServer = tabletServer;
    
    columnValuesForRowScan = new List[columnNames.length];
    
    for(int i = 0; i < columnNames.length; i++) {
      ColumnValue columnValue = next(i);
      if(columnValue == null) {
        if(tabletServer != null) {
          tabletServer.closeScanner(scanners[i].getScannerId());
        }
        scanners[i] = null;
      } else {
        columnValueBuf.add(new ColumnValueItem(columnValue, i));
      }
      
      columnNameIndex.put(columnNames[i], i);
      columnValuesForRowScan[i] = new ArrayList<ColumnValue>();
    }
    
    for(int i = 0; i < columnNames.length; i++) {
      columnMap.put(columnNames[i], i);
    }
  }
  
  public String[] getColumnNames() {
    return columnNames;
  }
  
  public void close() throws IOException {
    close(true);
  }

  public void close(boolean checkAction) throws IOException {
    for(int i = 0; i < scanners.length; i++) {
      try {
        if(scanners[i] != null && tabletServer != null) {
          tabletServer.closeScanner(scanners[i].getScannerId(), checkAction);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }
  
  private ColumnValue next(int columnIndex) throws IOException {
    List<ColumnValue> values = columnScanBuf.get(columnIndex);
    if(values == null || values.isEmpty()) {
      if(scanners[columnIndex] == null) {
        return null;
      }
      ColumnValue[] columnValues = scanners[columnIndex].next();
      if(tabletServer != null) {
        tabletServer.leaseHolder.touch(scanners[columnIndex].getScannerId());
      }
      
      if(columnValues == null || columnValues.length == 0) {
        return null;
      } else {
        if(values == null) {
          values = new ArrayList<ColumnValue>();
          columnScanBuf.put(columnIndex, values);
        }
        for(ColumnValue eachColumnValue: columnValues) {
          values.add(eachColumnValue);
        }
      }
    }
    return values.remove(0);
  }
  
  public RowColumnValues next() throws IOException {
    if(columnValueBuf.isEmpty()) {
      return null;
    }
    
    ColumnValueItem columnValueItem = columnValueBuf.first();
    columnValueBuf.remove(columnValueItem);
    int winnerIndex = columnValueItem.scannerIndex;
    ColumnValue columnValue = columnValueItem.columnValue;
    RowColumnValues rowColumnValues = 
      new RowColumnValues(columnNames[winnerIndex], 
                          columnValue.getRowKey(), 
                          new ColumnValue[]{columnValue});
    
    if(scanners[winnerIndex] != null) {
      columnValue = next(winnerIndex);
      if(columnValue == null) {
        if(tabletServer != null) {
          tabletServer.closeScanner(scanners[winnerIndex].getScannerId());
        }
        scanners[winnerIndex] = null;
      } else {
        columnValueBuf.add(new ColumnValueItem(columnValue, winnerIndex));
      }      
    }
    return rowColumnValues;
  }

  public RowColumnValues[] nextRow() throws IOException {
    Row.Key currentRowKey = null;
    RowColumnValues rowColumnValue = null;
    while(true) {
      rowColumnValue = next();
      if(isEmptyValue(rowColumnValue)) {
        break;
      }
      
      if(this.previousRowKey == null) {
        previousRowKey = rowColumnValue.getRowKey();
      }

      currentRowKey = rowColumnValue.getRowKey();
      if(previousRowKey.equals(currentRowKey)) {
        int columnIndex = columnNameIndex.get(rowColumnValue.getColumnName());
        columnValuesForRowScan[columnIndex].add(rowColumnValue.getColumnRecords()[0]);
      } else {
        break;
      }
    } //end of while
    
    if(previousRowKey == null) {
      return null;
    }
    
    RowColumnValues[] rowColumnValues = new RowColumnValues[columnNames.length];
    boolean hasData = false;
    for(int i = 0; i < columnNames.length; i++) {
      rowColumnValues[i] = new RowColumnValues();
      rowColumnValues[i].setRowKey(previousRowKey);
      rowColumnValues[i].setColumnName(columnNames[i]);
      rowColumnValues[i].setColumnRecords(columnValuesForRowScan[i].toArray(new ColumnValue[]{}));
      
      if(columnValuesForRowScan[i].size() > 0) {
        hasData = true;
      }
      
      numScannedColumnValue += columnValuesForRowScan[i].size();
      columnValuesForRowScan[i].clear();
    }
    
    if(!isEmptyValue(rowColumnValue)) {
      int columnIndex = columnNameIndex.get(rowColumnValue.getColumnName());
      columnValuesForRowScan[columnIndex].add(rowColumnValue.getColumnRecords()[0]);
    }
    previousRowKey = currentRowKey;
    
    if(hasData) {
      return rowColumnValues;
    } else {
      return null;
    }
  }

  private boolean isEmptyValue(RowColumnValues rowColumnValue) {
    return rowColumnValue == null || 
        rowColumnValue.getColumnRecords() == null ||
        rowColumnValue.getColumnRecords().length == 0;
  }

  class ColumnValueItem implements Comparable<ColumnValueItem> {
    protected ColumnValue columnValue;

    protected Integer scannerIndex;

    public ColumnValueItem(ColumnValue columnValue, int scannerIndex) {
      this.columnValue = columnValue;
      this.scannerIndex = scannerIndex;
    }

    public Row.Key getRowKey() {
      return columnValue.getRowKey();
    }

    public int compareTo(ColumnValueItem item) {
      int compare = columnValue.getRowKey().compareTo(item.getRowKey());
      if (compare == 0) {
        return scannerIndex.compareTo(item.scannerIndex);
      } else
        return compare;
    }
  }

  public int getNumScannedColumnValue() {
    return numScannedColumnValue;
  }
}