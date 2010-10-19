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
package org.cloudata.core.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.cloudata.core.client.CellFilter.CellPage;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.util.StringUtils;


/**
 * Row is Value Object which represents Table's row.<BR>
 * A Row object has multiple columns. <BR>
 * Each column has multiple Cell Objects<BR>
 */
public class Row implements CWritable, Comparable<Row>, Writable {
  private static final Log LOG = LogFactory.getLog(Row.class.getName());
  
  private Key key;
  
  //columnName -> Cells
  private Map<String, TreeMap<Cell.Key, Cell>> cells = new HashMap<String, TreeMap<Cell.Key, Cell>>();
  
  public Row() {
  }
  
  public Row(Row.Key key) {
    this.key = key;
  }
  
  /**
   * @deprecated
   * Use getColumnCount() 
   * @return
   */
  public int getColumnSize() {
    return getColumnCount();
  }
  /**
   * return number of columns(NTable.get() or scan)
   * @return
   */
  public int getColumnCount() {
    return cells.size();
  }
  
  /**
   * Return Row.Key
   * @return
   */
  public Key getKey() {
    return key;
  }
  
  /**
   * Add Cell to specified column
   * @param columnName
   * @param newCell
   */
  public void addCell(String columnName, Cell newCell) {
    TreeMap<Cell.Key, Cell> cellMap = cells.get(columnName);
    if(cellMap == null) {
      cellMap = new TreeMap<Cell.Key, Cell>();
      cells.put(columnName, cellMap);
    }
    
    internalAddCell(cellMap, newCell);
  }
  
  /**
   * @deprecated
   * Use addCellList()
   */
  public void addCells(String columnName, List<Cell> paramCells) {
    addCellList(columnName, paramCells);
  }
  
  /**
   * Add Cell list to specified column
   * @param columnName
   * @param paramCells
   */
  public void addCellList(String columnName, List<Cell> paramCells) {
    TreeMap<Cell.Key, Cell> cellMap = cells.get(columnName);
    if(cellMap == null) {
      cellMap = new TreeMap<Cell.Key, Cell>();
      cells.put(columnName, cellMap);
    }
    
    for(Cell eachCell: paramCells) {
      internalAddCell(cellMap, eachCell);
    }
  }
  
  private void internalAddCell(TreeMap<Cell.Key, Cell> cellMap, Cell newCell) {
    Cell cell = null;
    if ((cell = cellMap.get(newCell.key)) == null) {
      cellMap.put(newCell.key, newCell);
    } else {
      cell.values.addAll(newCell.values);
    }
  }
  
  /**
   * @deprecated
   * Use getCellList()
   * @param columnName
   * @return
   */
  public List<Cell> getCells(String columnName) {
    return getCellList(columnName);
  }

  /**
   * Return Cell object.
   * @param columnName
   * @param cellKey
   * @return cell 객체
   */
  public Cell getCell(String columnName, Cell.Key cellKey) {
    TreeMap<Cell.Key, Cell> cellMap = cells.get(columnName);
    Cell cell = null;
    
    if (cellMap == null || (cell = cellMap.get(cellKey)) == null) {
      return null;
    }
    
    return cell;
  }
  
  /**
   * Return cell list 
   * @param columnName
   * @return
   */
  public List<Cell> getCellList(String columnName) {
    TreeMap<Cell.Key, Cell> cellList = cells.get(columnName);
    if(cellList == null) {
      return null;
    }
    
    return new ArrayList<Cell>(cellList.values());
  }
  
  /**
   * @deprecated
   * Use getCellMap()
   * @param columnName
   * @return
   */
  public TreeMap<Cell.Key, Cell> getCellMaps(String columnName) {
    return getCellMap(columnName);
  }

  
  /**
   * Return Map of Cells
   * @param columnName
   * @return
   */
  public TreeMap<Cell.Key, Cell> getCellMap(String columnName) {
    return cells.get(columnName);
  }
  
  /**
   * @deprecated
   * use getFirst()
   * @param columnName
   * @return
   */
  public Cell getOne(String columnName) {
    return getFirst(columnName);
  }
  
  /**
   * Return first Cell object in specified column<BR>
   * @param columnName
   * @return
   */  
  public Cell getFirst(String columnName) {
    TreeMap<Cell.Key, Cell> cellMap = cells.get(columnName);
    if(cellMap == null || cellMap.isEmpty()) {
      return null;
    }
    
    return cellMap.firstEntry().getValue();
  }
  
  /**
   * @deprecated
   * Use getFirstColumnCellList()
   * @return
   * @throws IOException
   */
  public List<Cell> getFirstColumnCells() throws IOException {
    return getFirstColumnCellList();
  }
  
  /**
   * Return Cell list<BR>
   * This method must be used when called NTable.get() or scan with single column.<BR>
   * If getColumnCount() > 1, throws IOExcepiton.
   * @return
   * @throws IOException
   */
  public List<Cell> getFirstColumnCellList() throws IOException {
    if(cells.isEmpty()) {
      return null;
    }

    if(cells.size() > 1) {
      throw new IOException("getFirstColumnCells avaliable when column is 1");
    }
    
    return getCellList(cells.keySet().iterator().next());
  }
  
  
  @Override
  public void readFields(DataInput in) throws IOException {
    key = new Key();
    key.readFields(in);
    cells.clear();
    
    int columnLength = in.readInt();
    for(int i = 0; i < columnLength; i++) {
      String columnName = CWritableUtils.readString(in);
      
      int columnCellsSize = in.readInt();
      TreeMap<Cell.Key, Cell> columnCells = new TreeMap<Cell.Key, Cell>();
      for(int j = 0; j < columnCellsSize; j++) {
        Cell cell = new Cell();
        cell.readFields(in);
        columnCells.put(cell.key, cell);
      }
      
      cells.put(columnName, columnCells);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    key.write(out);
    
    out.writeInt(cells.size());
    
    for(Map.Entry<String, TreeMap<Cell.Key, Cell>> entry: cells.entrySet()) {
      String columnName = entry.getKey();
      CWritableUtils.writeString(out, columnName);
      TreeMap<Cell.Key, Cell> columnCells = entry.getValue();
      
      int columnCellsSize = columnCells == null ? 0 : columnCells.size();
      out.writeInt(columnCellsSize);
      if(columnCellsSize > 0) {
        for(Cell eachCell: columnCells.values()) {
          //System.out.println(columnName + ">" + eachCell.getKey() + ">" + eachCell.getValueAsString());
          eachCell.write(out); 
        }
      }
    }
  }

  @Override
  public int compareTo(Row row) {
    return key.compareTo(row.key);
  }

  public boolean equals(Object obj) {
    if( !(obj instanceof Row) ) {
      return false;
    }
    
    return compareTo((Row)obj) == 0;
  }  
  
  /**
   * Row.Key class
   *
   */
  public static class Key extends AbstractKey {
    public static final Key MIN_KEY = new Key();
    public static final Key MAX_KEY = new Key();  
    static {
      try {
        MIN_KEY.set(Constants.MIN_VALUE);
        MAX_KEY.set(Constants.MAX_VALUE);
      } catch (Exception e) {
      }
    }
    
    public Key() {
      super();
    }
    
    public Key(byte[] bytes) {
      this(bytes, 0, bytes.length);
    }
    
    public Key(byte[] bytes, int pos, int length) {
      super();
      set(bytes, pos, length);
    }
    
    public Key(String key) {
      this(key.getBytes());
    }
    
    public Key(Key otherKey) {
      super();
      set(otherKey.getBytes(), 0, otherKey.getLength());
    }
    
    public boolean isMaxValue() {
      return MAX_KEY.equals(this);
    }
  }

  public Map<String, TreeMap<Cell.Key, Cell>> getAllCells() {
    return cells;
  }

  public void setAllCells(Map<String, TreeMap<Cell.Key, Cell>> cells) {
    this.cells = cells;
  }

  public void setKey(Key key) {
    this.key = key;
  }

  public String[] getColumnNames() {
    if(cells == null || cells.isEmpty()) {
      return null;
    }
    
    String[] columnNames = new String[cells.size()];
    int index = 0;
    for(String eachColumn: cells.keySet()) {
      columnNames[index++] = eachColumn;
    }
    return columnNames;
  }
  
  public void print(String columnName) {
    Collection<Cell> cells = getCellList(columnName);
    System.out.println("--------------- " + columnName + " -------------------");
    if(cells == null || cells.isEmpty()) {
      System.out.println("No data in " + getKey());
    } else { 
      for(Cell eachCell: cells) {
        List<Cell.Value> values = eachCell.getValues();
        int valueCount = (values == null) ? 0 : values.size();
        System.out.println("Row.Key[" + getKey() + "] Cell.Key[" + eachCell.getKey() + "] Cell.Value.size():" + valueCount);
        
        if(valueCount == 0) {
          System.out.println("No data in cell:" + eachCell.getKey());
          continue;
        }
        
        for(Cell.Value eachValue: values) {
          System.out.println(eachValue.getValueAsString());
        }
      }
    }    
  }

  /**
   * Return max number of Cell.Value 
   * @return
   */
  public int getMaxNumCellValue() {
    int result = 0;
    String[] columNames = getColumnNames();
    if(columNames == null || columNames.length == 0) {
      return 0;
    }
    
    for(String columnName: columNames) {
      Collection<Cell> cells = getCellList(columnName);
      if(cells != null && cells.size() > 0) {
        for(Cell eachCell: cells) {
          List<Cell.Value> values = eachCell.getValues();
          result += (values == null) ? 1 : values.size();
        }
      }
    }
    return result;
  }

  /**
   * Return CellPage object for next page cell datas.
   * @param columnName
   * @param pageSize
   * @return if no data in rows, return empty CellPage(pageSize, null, 0) object
   */
  public CellPage getNextCellPage(String columnName, int pageSize) {
    List<Cell> columnCells = getCellList(columnName);
    if(columnCells == null || columnCells.size() == 0) {
      return new CellPage(pageSize, Cell.Key.MAX_KEY, 0);
    }
    
    Cell cell = columnCells.get(columnCells.size() - 1);
    Cell.Value value = cell.getValue();
    
    Cell.Key cellkey = cell.getKey();
    long timestamp = 0;
    if(value != null) {
      timestamp = value.getTimestamp();
    }
    
    return new CellPage(pageSize, cellkey, timestamp);
  } 
  
  public boolean deepEquals(Row row) {
    if(row == null || row.key == null) {
      return false;
    }
    if(!key.equals(row.key)) {
      return false;
    }

    if(cells.size() != row.cells.size()) {
      return false;
    }
    
    for(Map.Entry<String, TreeMap<Cell.Key, Cell>> entry: cells.entrySet()) {
      String columnName = entry.getKey();
      TreeMap<Cell.Key, Cell> columnCells = entry.getValue();
      
      TreeMap<Cell.Key, Cell> targetColumnCells = row.getCellMap(columnName);
      
      int columnCellsSize = columnCells == null ? 0 : columnCells.size();
      int targetColumnCellsSize = targetColumnCells == null ? 0 : targetColumnCells.size();
      if(columnCellsSize != targetColumnCellsSize) {
        return false;
      }
      
      if(columnCellsSize > 0) {
        for(Cell eachCell: columnCells.values()) {
          Cell targetCell = targetColumnCells.get(eachCell.getKey());
          if(!eachCell.equals(targetCell)) {
            return false;
          }
          
          List<Cell.Value> values = eachCell.getValues();
          List<Cell.Value> targetValues = targetCell.getValues();
          
          int valueSize = values == null ? 0 : values.size();
          int targetValueSize = targetValues == null ? 0 : targetValues.size();
          if(valueSize != targetValueSize) {
            return false;
          }
          
          for(int i = 0; i < valueSize; i++) {
            Cell.Value value = values.get(i);
            Cell.Value targetValue = values.get(i);
            
            if(!StringUtils.equalsBytes(value.getBytes(), targetValue.getBytes())) {
              return false;
            }
            
            if(value.isDeleted() != targetValue.isDeleted()) {
              return false;
            }
          }
        }
      }
    }
    
    return true;
  }
}

