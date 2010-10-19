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
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * Target Tablet: 테이블의 모든 Tablet <br>
 * Target Column: multiple column <br>
 * 내부적으로 컬럼별로 MultipleTabletScanner 객체를 가지고 있다. 
 * <b>주의사항: 오픈된 하나의 Scanner로 클라이언트에서 멀티 쓰레드에서 작업할 경우 정확한 동작을 보장하지 않는다.</b>
 * @author 김형준
 *
 */
class MultiTabletMultiColumnScanner implements TableScanner {
  private static final Log LOG = LogFactory.getLog(MultiTabletMultiColumnScanner.class.getName());
  
  private String[] columnNames;
  
  private MultiTabletScanner[] scanners;
  
  private SortedSet<RowItem> rowBufForNextRow;
  
  private SortedSet<ScanCellItem> scanCellBufForNext = new TreeSet<ScanCellItem>();
  
  private Map<String, Integer> columnMap = new HashMap<String, Integer>(); 
  
  private boolean first = true;
  
  private RowFilter rowFilter;
  
  private boolean end = false;
  
  private int timeout;
  
  protected MultiTabletMultiColumnScanner(
      CTable ctable, 
      RowFilter rowFilter,
      int timeout) throws IOException {
    this.rowFilter = rowFilter;
    this.columnNames = rowFilter.getColumns();
    this.timeout = timeout;
    
    scanners = new MultiTabletScanner[columnNames.length];
    for(int i = 0; i < columnNames.length; i++) {
      scanners[i] = new MultiTabletScanner(ctable, rowFilter.copyRowFilter(columnNames[i]), timeout);
    }
    
    for(int i = 0; i < columnNames.length; i++) {
      columnMap.put(columnNames[i], i);
    }
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  /**
   * 다음 데이터를 fetch한다.
   * @return  
   * @throws IOException
   */
  public ScanCell next() throws IOException {
    if(first) {
      first = false;
      for(int i = 0; i < columnNames.length; i++) {
        ScanCell scanCell = scanners[i].next();
        if(scanCell != null) {
          scanCellBufForNext.add(new ScanCellItem(scanCell, i));
        } else {
          scanners[i].close();
          scanners[i] = null;
        }
      }
    }
    if(scanCellBufForNext.size() == 0) {
      return null;
    }
    
    ScanCellItem scanCellItem = scanCellBufForNext.first();
    scanCellBufForNext.remove(scanCellItem);
    int winnerIndex = scanCellItem.scannerIndex;
    ScanCell result = scanCellItem.scanCell;
    
    if(scanners[winnerIndex] != null) {
      ScanCell scanCell = scanners[winnerIndex].next();
      if(scanCell == null) {
        scanners[winnerIndex].close();
        scanners[winnerIndex] = null;
      } else {
        scanCellBufForNext.add(new ScanCellItem(scanCell, winnerIndex));
      }      
    }
    
    return result;
  }

  
  /**
   * 특정 column의 다음 데이터를 fetch한다.
   * @param columnName
   * @return 해당 컬럼에 더이상 데이터가 없는 경우 null을 반환한다.
   * @throws IOException
   */
//  public ScanCell next(String columnName) throws IOException {
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
//  public ScanCell next(int columnIndex) throws IOException {
//    if(columnIndex >= columnNames.length) {
//      throw new IOException("invalid column index: " + columnIndex);
//    }
//    
//    if(scanners[columnIndex] == null) {
//      return null;
//    }
//    
//    ScanCell scanCell = scanners[columnIndex].next();
//    if(scanCell == null) {
//      scanners[columnIndex].close();
//      scanners[columnIndex] = null;
//    }
//    
//    return scanCell;
//  }

  /**
   * 현재 커서의 위치에 있는 row의 모든 컬럼, 모든 데이터를 fetch한다.<br>
   * (내부적으로 n개의 scanner로부터 merge sort를 사용한다.)<br>
   * 데이터가 많은 경우 메모리 부족 문제가 발생할 수 있다.<br>
   * @return 더 이상 값이 없으면 null을 반환
   * @throws IOException
   */
  public Row nextRow() throws IOException {
    if(end) {
      return null;
    }
    
    if(first) {
      first = false;
      rowBufForNextRow = new TreeSet<RowItem>();
      
      for(int i = 0; i < columnNames.length; i++) {
        if(scanners[i] != null) {
          Row row = scanners[i].nextRow();
          if(row != null && row.getColumnCount() != 0) {
            rowBufForNextRow.add(new RowItem(row, i));
          } else {
            scanners[i].close();
            scanners[i] = null;
          }
        }
      }
    }
    
    if(rowBufForNextRow.size() == 0) {
      return null;
    }
    
    RowItem winnerRowItem = rowBufForNextRow.first();
    Row.Key winnerRowKey = winnerRowItem.getRowKey();
    
    Row resultRow = new Row(winnerRowKey);
    
    //buf내에 동일한 rowkey를 가지고 있는 항목 모두 가져와서 하나로 merge한다.
    List<RowItem> tempItems = new ArrayList<RowItem>();
    tempItems.addAll(rowBufForNextRow);
    
    for(RowItem eachItem: tempItems) {
      Row.Key entryRowKey = eachItem.getRowKey();
      if(entryRowKey.equals(winnerRowKey)) {
        String columnName = columnNames[eachItem.scannerIndex];
        //System.out.println("ColumnName:" + columnName + "," + eachItem.row.getCells(columnName).size());
        resultRow.addCellList(columnName, eachItem.row.getCellList(columnName));
        
        rowBufForNextRow.remove(eachItem);
       
        if(scanners[eachItem.scannerIndex] != null) {
          Row row = scanners[eachItem.scannerIndex].nextRow();
          if(row != null && row.getColumnCount() != 0) {
            rowBufForNextRow.add(new RowItem(row, eachItem.scannerIndex));
          } else {
            scanners[eachItem.scannerIndex].close();
            scanners[eachItem.scannerIndex] = null;
          }
        }
      }
    }
    
    //endRowKey까지 읽은 경우 다음 레코드를 read하지 않기 위해 end 처리한다.
    //다음 레코드가 다른 Tablet에 있는 경우 Scanner를 오픈시키지 않기 위해서이다.
    if(resultRow.getKey().equals(rowFilter.getEndRowKey())) {
      end = true;
    }
    return resultRow;
  }

  /**
   * 커서를 다음 tablet으로 강제로 이동시킨다.
   * @throws IOException
   */
  public void nextTablet() throws IOException {
    for(int i = 0; i < columnNames.length; i++) {
      if(scanners[i] != null) {
        if(scanners[i].moveNextTablet() == null) {
          if(scanners[i] != null) {
            scanners[i].close();
          }
          scanners[i] = null;
        }
      }
    }
    
    //row단위로 scanner가 scan 된 경우
    if(rowBufForNextRow != null) {
      rowBufForNextRow.clear();
      for(int i = 0; i < columnNames.length; i++) {
        if(scanners[i] != null) {
          Row row = scanners[i].nextRow();
          if(row != null && row.getColumnCount() != 0) {
            rowBufForNextRow.add(new RowItem(row, i));
          } else {
            scanners[i].close();
            scanners[i] = null;
          }
        }
      }
    }
  }

  /**
   * Scanner를 close한다.
   * @throws IOException
   */
  public void close() throws IOException {
    for(int i = 0; i < columnNames.length; i++) {
      try {
        if(scanners[i] != null) {
          scanners[i].close();
        }
      } catch (IOException e) {
        LOG.error("Close Error:" + e.getMessage(), e);
      }
    }
  }
  
//  private boolean isEmptyValue(RowColumnValues rowColumnValue) {
//    return rowColumnValue == null || 
//        rowColumnValue.getColumnRecords() == null ||
//        rowColumnValue.getColumnRecords().length == 0;
//  }
 
  class RowItem implements Comparable<RowItem> {
    private Row row;
    private Integer scannerIndex;
    
    public RowItem(Row row, int scannerIndex) {
      this.row = row;
      this.scannerIndex = scannerIndex;
    }
    
    public Row.Key getRowKey() {
      return row.getKey();
    }

    public int compareTo(RowItem item) {
      int compare = row.compareTo(item.row);
      if(compare == 0) {
        return scannerIndex.compareTo(item.scannerIndex);
      }
      else return compare;
    }
    
    public boolean equals(Object obj) {
      if( !(obj instanceof RowItem) ) {
        return false;
      }
      
      return compareTo((RowItem)obj) == 0;
    }    
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      System.out.println("usage: MultiTabletMultiColumnScanner <table name> <column name> <printyn(Y|N)> [startRowKey] [endRowKey]");
      return;
    }
    selectByRow(args);
  }
  
  private static void selectByRow(String[] args) throws IOException {
    CTable ctable = CTable.openTable(new CloudataConf(), args[0]);
    
    String[] columns = args[1].split(",");
    boolean print = false;

    if("Y".equals(args[2])) {
      print = true;
    }
    
    TableScanner scanner = null;
    
    try {
      
      if(args.length > 4) {
        Row.Key startRowKey = new Row.Key(args[3]);
        Row.Key endRowKey = new Row.Key(args[4]);
        if("MIN".equals(args[3])) {
          startRowKey = Row.Key.MIN_KEY;
        }
        if("MAX".equals(args[4])) {
          endRowKey = Row.Key.MAX_KEY;
        }
        scanner = ScannerFactory.openScanner(ctable, startRowKey, endRowKey, columns);
      } else {
        scanner = ScannerFactory.openScanner(ctable, columns);
      }
      
      int count = 0;
      
      Row row = null;
      System.out.println("Start scan");
      while( (row = scanner.nextRow()) != null) {
        if(print) {
          List<Cell> cells = row.getCellList(columns[0]);
          System.out.println("---------------------------------------");
          if(cells == null || cells.size() == 0) {
            System.out.println("No data in " + row.getKey());
          } else { 
            for(Cell eachCell: cells) {
              System.out.println(row.getKey() + "," + eachCell.getKey() + "," + eachCell.getValue().getValueAsString());
            }
          }
        }
        if(row.getKey().toString().indexOf("The H.K. Triad") >= 0) {
          List<Cell> cells = row.getCellList(columns[0]);
          System.out.println("---------------------------------------");
          if(cells == null || cells.size() == 0) {
            System.out.println("No data in " + row.getKey());
          } else { 
            for(Cell eachCell: cells) {
              System.out.println(row.getKey() + "," + eachCell.getKey() + "," + eachCell.getValue().getValueAsString());
            }
          }
          cells = row.getCellList(columns[1]);
          System.out.println("---------------------------------------");
          if(cells == null || cells.size() == 0) {
            System.out.println("No data in " + row.getKey());
          } else { 
            for(Cell eachCell: cells) {
              System.out.println(row.getKey() + "," + eachCell.getKey() + "," + eachCell.getValueSize());
            }
          }
        }
        count++;
        if(count % 1000 == 0) {
          System.out.println(count + " rows scaned");
        }
      }
      System.out.println("Scan End:" + count + " rows scaned");
    } catch (Exception e) {
      System.out.println("=========================================");
      e.printStackTrace(System.out);
      System.out.println("=========================================");
    } finally {
      scanner.close();
    }
  }
}
