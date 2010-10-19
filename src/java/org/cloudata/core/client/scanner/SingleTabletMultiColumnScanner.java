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
package org.cloudata.core.client.scanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;



/**
 * Target Tablet: 하나의 Tablet <br>
 * Target Column: multiple column <br>
 * 주로 MapReducde 작업 시 TabletInputFormat에 의해 사용된다.
 * 특정 tablet에 대해서만 scan을 수행하고 여러개의 컬럼에 대해 오픈한 경우 특정 row에 대해 
 * 하나의 컬럼에 대한 데이터를 fetch하고 해당 컬럼에 더이상 데이터가 없는 다음 컬럼 데이터를 fetch하는 순서로 scan 된다.  
 * 하나의 row, columnkey에 여러개의 데이터가 있어도 하나씩 fetch 된다.
 * <b>주의사항: 오픈된 하나의 Scanner로 클라이언트에서 멀티 쓰레드에서 작업할 경우 정확한 동작을 보장하지 않는다.</b>
 *   
 * @author 김형준
 *
 */
class SingleTabletMultiColumnScanner implements TableScanner, Constants {
  private static final Log LOG = LogFactory.getLog(SingleTabletMultiColumnScanner.class.getName());

  private String[] columnNames;
  
  private TableScanner[] scanners;

  private SortedSet<ScanCellItem> scanCellValueBuf = new TreeSet<ScanCellItem>();
  
//  private boolean first = true;
  
  private Row.Key previousRowKey;
  
  private List<ScanCell>[] cellForRowScan;
  
  private Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
  
  private Map<String, Integer> columnMap = new HashMap<String, Integer>(); 
  
  @SuppressWarnings("unchecked")
  protected SingleTabletMultiColumnScanner(CloudataConf conf, 
                                        Row.Key targetRowKey,
                                        TabletInfo tabletInfo, 
                                        RowFilter rowFilter) throws IOException {
    this.columnNames = rowFilter.getColumns();
    scanners = new SingleTabletScanner[columnNames.length];
    
    cellForRowScan = new List[columnNames.length];
    
    for(int i = 0; i < columnNames.length; i++) {
      scanners[i] = ScannerFactory.openScanner(conf, targetRowKey, 
          tabletInfo, rowFilter.copyRowFilter(columnNames[i]));
      
      ScanCell scanCell = scanners[i].next();
      if(scanCell == null) {
        scanners[i].close();
        scanners[i] = null;
      } else {
        scanCellValueBuf.add(new ScanCellItem(scanCell, i));
      }
      
      columnNameIndex.put(columnNames[i], i);
      cellForRowScan[i] = new ArrayList<ScanCell>();
    }
    
    for(int i = 0; i < columnNames.length; i++) {
      columnMap.put(columnNames[i], i);
    }
  }
  
  public String[] getColumnNames() {
    return columnNames;
  }
  
  public void close() throws IOException {
    for(int i = 0; i < scanners.length; i++) {
      try {
        if(scanners[i] != null) {
          scanners[i].close();
        }
      } catch (Exception e) {
        
      }
    }
  }

//  public ColumnValue next(String columnName) throws IOException {
//    if(!columnMap.containsKey(columnName)) {
//      throw new IOException("No column: " + columnName);
//    }
//    return next(columnMap.get(columnName));
//  }
  
  /**
   * 특정 column의 다음 데이터를 fetch한다.
   * @param columnIndex
   * @return 해당 컬럼에 더이상 데이터가 없는 경우 null을 반환한다.
   * @throws IOException
   */
//  public ColumnValue next(int columnIndex) throws IOException {
//    if(columnIndex >= columnNames.length) {
//      throw new IOException("invalid column index: " + columnIndex);
//    }
//    
//    if(scanners[columnIndex] == null) {
//      return null;
//    }
//    
//    ColumnValue columnValue = scanners[columnIndex].next();
//    if(columnValue == null) {
//      scanners[columnIndex].close();
//      scanners[columnIndex] = null;
//    }
//    
//    return columnValue;
//  }
  
  /**
   * 다음 데이터를 fetch한다.
   * @return 
   * @throws IOException
   */
  public ScanCell next() throws IOException {
    if(scanCellValueBuf.size() == 0) {
      return null;
    }
    
    ScanCellItem scanCellItem = scanCellValueBuf.first();
    scanCellValueBuf.remove(scanCellItem);
    int winnerIndex = scanCellItem.scannerIndex;
    ScanCell result = scanCellItem.scanCell;
    
    if(scanners[winnerIndex] != null) {
      ScanCell scanCell = scanners[winnerIndex].next();
      if(scanCell == null) {
        scanners[winnerIndex].close();
        scanners[winnerIndex] = null;
      } else {
        scanCellValueBuf.add(new ScanCellItem(scanCell, winnerIndex));
      }      
    }
    return result;
  }

  
  /**
   * 현재 커서의 위치에 있는 row의 모든 컬럼, 모든 데이터를 fetch한다.<br>
   * (내부적으로 n개의 scanner로부터 merge sort를 사용한다.)<br>
   * 데이터가 많은 경우 메모리 부족 문제가 발생할 수 있다.<br>
   * @return 더 이상 값이 없으면 null을 반환
   * @throws IOException
   */
  public Row nextRow() throws IOException {
    Row.Key currentRowKey = null;
    ScanCell scanCell = null;
    while(true) {
      scanCell = next();
      if(scanCell == null) {
        break;
      }
      
      if(this.previousRowKey == null) {
        previousRowKey = scanCell.getRowKey();
      }

      currentRowKey = scanCell.getRowKey();
      if(previousRowKey.equals(currentRowKey)) {
        int columnIndex = columnNameIndex.get(scanCell.getColumnName());
        cellForRowScan[columnIndex].add(scanCell);
      } else {
        break;
      }
    } //end of while
    
    if(previousRowKey == null) {
      return null;
    }
    
    Row resultRow = new Row(previousRowKey);
    boolean hasData = false;
    for(int i = 0; i < columnNames.length; i++) {
      if(cellForRowScan[i].size() > 0) {
        hasData = true;
      }
      ScanCell.addCells(resultRow, columnNames[i], cellForRowScan[i]);     
      cellForRowScan[i].clear();
    }
    
    if(scanCell != null) {
      int columnIndex = columnNameIndex.get(scanCell.getColumnName());
      cellForRowScan[columnIndex].add(scanCell);
    }
    previousRowKey = currentRowKey;
    
    if(hasData) {
      return resultRow;
    } else {
      return null;
    }
  }

  public static void main(String[] args) throws IOException {
    if(args.length < 2) {
      System.out.println("Usage: java SingleTabletMultiColumnScanner <table name> <column name> (tablet_name)");
      return;
    }
    
    String[] columns = args[1].split(",");
    
    CTable ctable = CTable.openTable(new CloudataConf(), args[0]);
    TabletInfo[] tabletInfos = ctable.listTabletInfos();
    
    RowFilter filter = new RowFilter();
    for(String eachColumn: columns) {
      CellFilter cellFilter = new CellFilter(eachColumn);
      cellFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);
      
      filter.addCellFilter(cellFilter);
    }
      
    String tabletName = null; 
    if(args.length > 2) {
      tabletName = args[2];
    }
    for(int i = 0; i < tabletInfos.length; i++) {
      if(tabletName != null) {
        if(!tabletInfos[i].getTabletName().equals(tabletName)) {
          continue;
        }
      }
      TableScanner scanner = new SingleTabletMultiColumnScanner(ctable.getConf(), 
          tabletInfos[i].getEndRowKey(), tabletInfos[i], filter);
      ScanCell scanCell = null;
      while ( (scanCell = scanner.next()) != null) {
          LOG.error(scanCell.getRowKey().toString() + "," + scanCell.getCellKey());
      }
      scanner.close();
      /*
      Row row = null;
      while( (row = scanner.nextRow()) != null ) {
	System.out.println(row.getKey().toString());
        //for(String eachColumn: columns) {
        //  row.print(eachColumn);
        //}
      }
      */
    }
  }
//  private boolean isEmptyValue(ScanCell scanCell) {
//    return scanCell == null || 
//            scanCell.getCell() == null ||
//            scanCell.getCell().getValueSize() == 0;
//  }
}
