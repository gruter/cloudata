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
import java.util.List;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;


/**
 * RowFilter and CellFilter are normally used to get or scan for complex search condition<BR>
 * <pre>
 * CTable ctable = CTable.openTable(...);
 * RowFilter rowFilter = new RowFilter(new Row.Key("rk1"), OP_LIKE);
 * CellFilter cellFilter = new CellFilter("Column1");
 * cellFilter.setStartTimestamp(Long.MIN_VALUE);
 * cellFilter.setEndTimestamp(Long.MAX_VALUE);
 * rowFilter.addCellFilter(cellFilter);
 *
 * Row[] rows = ctable.gets(rowFilter);
 * </pre>
 *
 */
public class RowFilter implements CWritable {
  public static final int OP_EQ = 0;
  public static final int OP_LIKE = 1;
  public static final int OP_BETWEEN = 2;
  
  private Row.Key startRowKey;
  private Row.Key endRowKey;
  private int operation;
  
  private List<CellFilter> cellFilters = new ArrayList<CellFilter>();

  private int pageSize;
  
  public RowFilter() {
    this(Row.Key.MIN_KEY, Row.Key.MAX_KEY);
  }

  public RowFilter(Row.Key rowKey) {
    this(rowKey, rowKey);
  }
  
  public RowFilter(Row.Key rowKey, int operation) {
    this(rowKey, rowKey, operation);
  }
  
  public RowFilter(Row.Key startRowKey, Row.Key endRowKey) {
    this(startRowKey, endRowKey, OP_EQ);
  }
  
  public RowFilter(Row.Key startRowKey, Row.Key endRowKey, int operation) {
    setValue(startRowKey, endRowKey, operation);
  }
  
  public void setValue(Row.Key startRowKey, Row.Key endRowKey, int operation) {
    this.startRowKey = startRowKey;
    this.endRowKey = endRowKey;
    this.operation = operation;
    
    if(operation == OP_LIKE) {
      byte[] endKeyBytes = new byte[startRowKey.getLength() + 1];
      System.arraycopy(startRowKey.getBytes(), 0, endKeyBytes, 0, startRowKey.getLength());
      
      endKeyBytes[startRowKey.getLength()] = (byte)0xff;
        
      this.endRowKey = new Row.Key(endKeyBytes);
    }
  }

  public void setPagingInfo(int pageSize, Row.Key previousPageEndRowKey) {
    this.pageSize = pageSize;
    if(previousPageEndRowKey != null) {
      this.startRowKey = new Row.Key(previousPageEndRowKey.nextKeyBytes());
    }
  }
  
  public int getPageSize() {
    return pageSize;
  }
  
  /**
   * @return the columnFilterInfos
   */
  public List<CellFilter> getCellFilters() {
    return cellFilters;
  }
  
  /**
   * 
   * @param columnFilter 
   */
  public void addCellFilter(CellFilter cellFilter) {
    this.cellFilters.add(cellFilter);
  }
  
  /**
   * @return the rowkey
   */
  public Row.Key getStartRowKey() {
    return startRowKey;
  }
  
  /**
   * @return the rowkey
   */
  public Row.Key getEndRowKey() {
    return endRowKey;
  }
  /**
   * 
   * @param rowKey
   */
  public void setStartRowKey(Row.Key startRowKey) {
    this.startRowKey = startRowKey;
  }
  /**
   * 
   * @param rowKey
   */
  public void setEndRowKey(Row.Key endRowKey) {
    this.endRowKey = endRowKey;
  }
  
  public void setRowKey(Row.Key rowKey) {
    this.startRowKey = rowKey;
    this.endRowKey = rowKey;
  }
  
  public Row.Key getRowKey() {
    return this.startRowKey;
  }
  
  public void readFields(DataInput in) throws IOException {
    startRowKey = new Row.Key();
    startRowKey.readFields(in);

    endRowKey = new Row.Key();
    endRowKey.readFields(in);
    
    operation = in.readInt();

    pageSize = in.readInt();
    
    int cellFilterInfoLength = in.readInt();
    for(int i = 0; i < cellFilterInfoLength; i++) {
      CellFilter cellFilter = new CellFilter();
      cellFilter.readFields(in);
      cellFilters.add(cellFilter);
    }
  }
  
  public void write(DataOutput out) throws IOException {
    if(cellFilters == null && cellFilters.size() == 0) {
      throw new IOException("Set column filter info");
    }
    startRowKey.write(out);
    endRowKey.write(out);
    
    out.writeInt(operation);
    
    out.writeInt(pageSize);
    
    out.writeInt(cellFilters.size());
    for(CellFilter columnFilter: cellFilters) {
      columnFilter.write(out);
    }    
  }
  
  public String[] getColumns() {
    String[] columns = new String[cellFilters.size()];
    int index = 0;
    for(CellFilter cellFilter: cellFilters) {
      columns[index++] = cellFilter.getColumnName();
    }
    return columns;
  }
  
  public CellFilter getCellFilterInfo(String columnName) {
    for(CellFilter cellFilter: cellFilters) {
      if(cellFilter.getColumnName().equals(columnName)) {
        return cellFilter;
      }
    }
    return null;
  }

  public void clearCellFilter() {
    cellFilters.clear();
  }
  
  public String toString() {
    String result = "RowFilter:" + startRowKey + " to " + endRowKey + "\n";
    for(CellFilter cellFilter: cellFilters) {
      result += cellFilter.toString() + "\n";
    }
    
    return result;
  }
  
  public RowFilter copyRowFilter() {
    RowFilter rowFilter = new RowFilter();
    rowFilter.setStartRowKey(startRowKey);
    rowFilter.setEndRowKey(endRowKey);
    for(CellFilter cellFilter: cellFilters) {
      rowFilter.addCellFilter(cellFilter);
    }
    return rowFilter;
  }
  
  public RowFilter copyRowFilter(String columnName) {
    RowFilter rowFilter = new RowFilter();
    rowFilter.setStartRowKey(startRowKey);
    rowFilter.setEndRowKey(endRowKey);
    rowFilter.addCellFilter(getCellFilterInfo(columnName));
    
    return rowFilter;
  }

  public int getOperation() {
    return operation;
  }
  
  public boolean isPaging() {
    return pageSize > 0;
  }
  
  public void checkFilterParam() throws IOException {
    for(CellFilter cellFilter: cellFilters) {
      String message = cellFilter.checkFilterParam();
      if(message != null) {
        throw new IOException(message);
      }
    }
  }
  
  public void addAllCellFilter(TableSchema tableSchema) {
    for(ColumnInfo eachColumn: tableSchema.getColumnInfoArray()) {
      addCellFilter(new CellFilter(eachColumn.getColumnName()));
    }
  }
}
