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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;
import org.cloudata.core.tabletserver.DiskSSTable.ColumnValueEntry;


public class RecordSearcher {
  public static final Log LOG = LogFactory.getLog(RecordSearcher.class.getName());

  private MemorySSTable memorySSTable;
  private DiskSSTable diskSSTable;
  private int numOfVersion;
  
  public RecordSearcher(MemorySSTable memorySSTable, DiskSSTable diskSSTable, int numOfVersion) {
    this.memorySSTable = memorySSTable;
    this.diskSSTable = diskSSTable;
    this.numOfVersion = numOfVersion;
  }
  
  public RecordSearcher(Tablet tablet) {
    this(tablet.memorySSTable, tablet.diskSSTable.get(), tablet.getTable().getNumOfVersion());
  }

  private List<Searchable> initSearchsers(Row.Key rowKey, CellFilter cellFilter) throws IOException {
    //반드시 MemorySSTable부터 add되어야 한다.
    //그렇지 않으면 MinorCompaction 도중에 get하는 경우 data가 있는데 없다고 return하는 경우가 발생한다.
    List<Searchable> searchers = new ArrayList<Searchable>();
    searchers.add(memorySSTable.getSearcher(rowKey, cellFilter));
    searchers.addAll(diskSSTable.getSearchers(rowKey, cellFilter)); 
    return searchers;
  }
  
  public ColumnValue search(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    CellFilter cellFilter = new CellFilter();
    cellFilter.setColumn(columnName, cellKey);
      
    ColumnValue[] result = searchColumnValues(rowKey, cellFilter);
    if(result != null && result.length > 0) {
      ColumnValue columnValue = result[0];
      return columnValue;
    } else {
      return null;
    }
  }
  
  public ColumnValue[] search(Row.Key rowKey, String columnName) throws IOException {
    CellFilter cellFilter = new CellFilter(columnName);
    
    return searchColumnValues(rowKey, cellFilter);
  }
  
  /**
   * 여러개의 컬럼에 대한 값을 반환한다.
   * 하나의 Row.Key, 하나의 컬럼에 여러개의 컬럼값이 저장되기 때문에 2차원 배열로 반환한다.
   * @param rowKey
   * @param columnNames
   * @return
   * @throws IOException
   */
  public ColumnValue[][] search(Row.Key rowKey, String[] columnNames) throws IOException {
    if(rowKey == null) {
      throw new IOException("rowkey is null");
    }
    //TODO 컬럼별로 RowKey가 다르게 저장되는 경우 merge하는 로직이 추가 되어야 함
    //TODO 속도 확인 필요
    ColumnValue[][] columnValues = new ColumnValue[columnNames.length][];
    
    for(int i = 0; i < columnNames.length; i++) {
      columnValues[i] = search(rowKey, columnNames[i]);
    }
    return columnValues;
  }

  public RowColumnValues[] search(RowFilter rowFilter) throws IOException {
    if(rowFilter.getRowKey() == null || rowFilter.getRowKey().isEmpty()) {
      throw new IOException("rowkey is null");
    }
    
    Row.Key rowKey = rowFilter.getRowKey();
    
    List<CellFilter> cellFilters = rowFilter.getCellFilters();
    RowColumnValues[] result = new RowColumnValues[cellFilters.size()];
    
    int index = 0;
    boolean hasData = false;
    for(CellFilter eachCellFilter: cellFilters) {
      RowColumnValues rowColumnValues = new RowColumnValues();
      rowColumnValues.setRowKey(rowKey);
      rowColumnValues.setColumnName(eachCellFilter.getColumnName());
      
      ColumnValue[] columnValues = searchColumnValues(rowKey, eachCellFilter);
      
      if(columnValues != null &&columnValues.length > 0) {
        hasData = true; 
      }
      rowColumnValues.setColumnRecords(columnValues);
      result[index++] = rowColumnValues;
    }

    if(!hasData) {
      return null;
    } else {
      return result;
    }
  }
  
  private ColumnValue[] searchColumnValues(Row.Key rowKey, CellFilter cellFilter) throws IOException {
    if(rowKey == null) {
      throw new IOException("rowkey is null");
    }
    
    List<Searchable> searchers = initSearchsers(rowKey, cellFilter);
    try {
      if(searchers.isEmpty()) {
        return null;
      }
      List<ColumnValue> columnValues = mergeColumnValueFromSearcher(searchers, cellFilter);
      
      if(columnValues == null || columnValues.isEmpty()) {
        return null;
      }

      ColumnValue[] result = new ColumnValue[columnValues.size()];
      columnValues.toArray(result);
      return result;
    } finally {
    }
  }
  
  private List<ColumnValue> mergeColumnValueFromSearcher(
      List<Searchable> searcherList, 
      CellFilter cellFilter) throws IOException {
    LinkedList<ColumnValueEntry> workPlace = new LinkedList<ColumnValueEntry>();
    Searchable[] searchers = searcherList.toArray(new Searchable[]{});

    try {
      initWorkPlace(workPlace, searchers);
      
      int numOfValues = cellFilter.getNumOfValues();
  
      List<ColumnValue> result = new ArrayList<ColumnValue>();
      ValueCollection currentColumnValues = new ValueCollection();
  
      Cell.Key previousColumnKey = null;
      ColumnValue previousColumnValue = null;    
      
      boolean end = false;

      while(!end) {
        int size = workPlace.size();
        if(size == 0) {
          break;
        }
        Collections.sort(workPlace);
        ColumnValueEntry winner = workPlace.removeFirst();
        
        if(previousColumnKey == null) {
          previousColumnKey = winner.columnValue.getCellKey();
        }
  
        //meets new ValueCollection
        if(!previousColumnKey.equals(winner.columnValue.getCellKey())) {
          currentColumnValues.moveMatchedValues(cellFilter, result);
          currentColumnValues = new ValueCollection();
        } 
          
        if(previousColumnValue == null || !previousColumnValue.equals(winner.columnValue)) {
          currentColumnValues.add(winner.columnValue, numOfVersion);
          previousColumnKey = winner.columnValue.getCellKey();
          previousColumnValue = winner.columnValue;
        }
        
        //fetch from winner, add workPlace
        end = !fetchFromWinner(workPlace, searchers, winner.index);
        
        if(result.size() >= numOfValues) {
          break;
        }
      } //end of while
      
      if(result.size() < numOfValues) {
        //check last value in while loop
        if(currentColumnValues.getValueSize() > 0) {
          currentColumnValues.moveMatchedValues(cellFilter, result);
        }
        return result;
      } else {
        return result.subList(0, numOfValues);  
      }
    } finally {    
      //scnner close
      for(int i = 0; i < searchers.length; i++) {
        if(searchers[i] != null) {
          try {
            searchers[i].close();
          } catch(IOException e) {
            LOG.error("Can't close scanner:" + searchers[i]);
          }
          searchers[i] = null;
        }
      }
    }    
  }

  /**
   * @param workPlace
   * @param searchers
   * @throws IOException
   */
  private void initWorkPlace(LinkedList<ColumnValueEntry> workPlace,
      Searchable[] searchers) throws IOException {
    for(int i = 0; i < searchers.length; i++) {
      if(searchers[i] != null) {
        ColumnValue columnValue = searchers[i].next();
 
        if(columnValue != null) {
          workPlace.add(new ColumnValueEntry(columnValue, i));
        } else {
          searchers[i].close();
          searchers[i] = null;
        }
      }
    }
  }

  private boolean fetchFromWinner(LinkedList<ColumnValueEntry> workPlace,
      Searchable[] searchers, int winnerIndex) throws IOException {
    ColumnValue columnValue = null;
    if(searchers[winnerIndex] != null) {
      columnValue = searchers[winnerIndex].next();
    }
    
    if(columnValue != null) {
      workPlace.add(new ColumnValueEntry(columnValue, winnerIndex));
      return true;
    } else {
      if(searchers[winnerIndex] != null) {
        searchers[winnerIndex].close();
      }
      searchers[winnerIndex] = null;
      
      boolean end = workPlace.isEmpty();
      for(int i = 0; i < searchers.length; i++) {
        if(searchers[i] != null) {
          columnValue = searchers[i].next();
          if(columnValue != null) {
            workPlace.add(new ColumnValueEntry(columnValue, i));
            end = false;
            break;
          } else {
            searchers[i].close();
            searchers[i] = null;
          }
        }
      }
      return !end;
    }
  }

  public boolean hasValue(String columnName, Row.Key rowKey, Cell.Key cellKey) throws IOException {
    //List<Searchable> searchers = initSearchsers(rowKey, columnName, null);
    if(memorySSTable.hasValue(rowKey, columnName, cellKey)) {
      return true;
    }
    return diskSSTable.hasValue(rowKey, columnName, cellKey);
  }
}
