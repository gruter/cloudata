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

import org.cloudata.core.common.exception.GetFailException;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * RowFilter and CellFilter are normally used to get or scan for complex search condition<BR>
 * Can't set CellValueMatcher with numOfVersions or timestamp
 * @see RowFilter
 */
public class CellFilter implements CWritable {
  private String columnName;

  private Cell.Key startCellKey;    //always not null
  
  private Cell.Key endCellKey;
  
  //if startTimestamp, endTimestamp is 0, scan lastest value
  //if startTimestamp = 0, endTimestamp = Long.MAX, scan all values 
  private long startTimestamp = 0;
  private long endTimestamp = 0;
  
  /**
   * Number of version in a Cell 
   */
  private int numOfVersions = 1;
  
  private CellPage cellPage;
  
  private CellValueMatcherInfo cellValueMatcherInfo;
  
  private String[] filterParams;
  
  public CellFilter() {
    columnName = "";
    startCellKey = Cell.Key.EMPTY_KEY;
    endCellKey = Cell.Key.MAX_KEY;
  }

  public CellFilter(String columnName) {
    this.columnName = columnName;
    this.startCellKey = Cell.Key.EMPTY_KEY;
    this.endCellKey = Cell.Key.MAX_KEY;
  }
  
  public CellFilter(String columnName, Cell.Key cellKey) {
    this.columnName = columnName;
    this.startCellKey = cellKey;
    this.endCellKey = cellKey;
    this.setCellKey(cellKey);
  }
  
  public CellFilter(String columnName, Cell.Key startCellKey, Cell.Key endCellKey) {
    this.columnName = columnName;
    setCellKeys(startCellKey, endCellKey);
  }

  public void setCellKeys(Cell.Key startCellKey, Cell.Key endCellKey) {
    this.startCellKey = startCellKey;
    if(startCellKey == null) {
      this.startCellKey = Cell.Key.EMPTY_KEY;
    }
    this.endCellKey = endCellKey;
    if(endCellKey == null) {
      this.endCellKey = Cell.Key.MAX_KEY;
    }
  }
  
  public boolean isKeyBetweenFilter() {
    return !startCellKey.equals(endCellKey);
  }

  public void setCellValueMatcher(Class<?> cellValueMatcherClass, int classVersion, String[] filterParams) throws IOException {
//    if(numOfVersions > 1 || hasTimestampFilter()) {
//      throw new IOException("Can't set CellValueMatcher with numOfVersions or timestamp");
//    }
    if(hasTimestampFilter()) {
      throw new IOException("Can't set CellValueMatcher with numOfVersions or timestamp");
    }
    this.cellValueMatcherInfo = new CellValueMatcherInfo();
    this.cellValueMatcherInfo.setVersion(classVersion);
    
    this.cellValueMatcherInfo.addClassInfo(cellValueMatcherClass);
    
    Class<?>[] declaredClasses = cellValueMatcherClass.getDeclaredClasses();
    
    if(declaredClasses != null) {
      for(Class<?> eachClass: declaredClasses) {
        this.cellValueMatcherInfo.addClassInfo(eachClass);
      }
    }
    
    this.filterParams = filterParams;
  }
  
  public CellValueMatcher getCellValueMatcher() throws IOException {
    if(this.cellValueMatcherInfo == null) {
      return null;
    }
    
    return cellValueMatcherInfo.getCellValueMatcher();
  }
  
  public void readFields(DataInput in) throws IOException {
    columnName = CWritableUtils.readString(in);
    startCellKey = new Cell.Key();
    startCellKey.readFields(in);
    
    endCellKey = new Cell.Key();
    endCellKey.readFields(in);
    
    numOfVersions = in.readInt();
    startTimestamp = in.readLong();
    endTimestamp = in.readLong();
    
    int flag = in.readInt();
    if(flag > 0) {
      cellPage = new CellPage();
      cellPage.readFields(in);
    }
    
    if(in.readBoolean()) {
      cellValueMatcherInfo = new CellValueMatcherInfo();
      cellValueMatcherInfo.readFields(in);
    }
    filterParams = CWritableUtils.readStringArray(in);
  }
  
  public String[] getFilterParams() {
    return filterParams;
  }

  public void write(DataOutput out) throws IOException {
    if(numOfVersions > 1 && hasTimestampFilter()) {
      throw new GetFailException("Can't set CellFilter with numOfVersions or timestamp");
    }
    
    CWritableUtils.writeString(out, columnName);
    startCellKey.write(out);
    endCellKey.write(out);

    out.writeInt(numOfVersions);
    out.writeLong(startTimestamp);
    out.writeLong(endTimestamp);
    
    if(cellPage == null) {
      out.writeInt(0);
    } else {
      out.writeInt(1);
      cellPage.write(out);
    }
    
    if(cellValueMatcherInfo != null) {
      out.writeBoolean(true);
      cellValueMatcherInfo.write(out);
    } else {
      out.writeBoolean(false);
    }
    
    CWritableUtils.writeStringArray(out, filterParams);
  }

  public Cell.Key getTargetCellKey() {
    if(cellPage != null) {
      if(cellPage.startCellKey != null && !cellPage.startCellKey.equals(Cell.Key.EMPTY_KEY)) {
        return cellPage.startCellKey;
      }
    }
    return startCellKey;
  }

  public void setCellKey(Cell.Key cellKey) {
    if(cellKey == null) {
      this.startCellKey = Cell.Key.EMPTY_KEY;
      this.endCellKey = Cell.Key.EMPTY_KEY;
    } else {
      this.startCellKey = cellKey;
      this.endCellKey = cellKey;
    }
  }

  public boolean matchKey(Cell.Key cellKey) {
    return cellKey.compareTo(getTargetCellKey()) >= 0 && 
            cellKey.compareTo(endCellKey) <= 0;
  }
  
  public void setColumn(String columnName, Cell.Key cellKey) {
    this.columnName = columnName;
    setCellKey(cellKey);
  }

  public String getColumnName() {
    return columnName;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public int getNumOfVersions() {
    return numOfVersions;
  }

  public Cell.Key getEndCellKey() {
    return endCellKey;
  }
  
  /**
   * Set number of version in a Cell
   * @param numOfVersions
   */
  public void setNumOfVersions(int numOfVersions) {
    this.numOfVersions = numOfVersions;
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public void setTimestamp(long startTimestamp, long endTimestamp) {
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }
  
  public boolean hasTimestampFilter() {
    return startTimestamp != 0 || endTimestamp != 0; 
  }

  public int getNumOfValues() {
    if(cellPage == null) {
      return Integer.MAX_VALUE;
    } else {
      return cellPage.pageSize;
    }
  }

  public CellPage getCellPage() {
    return cellPage;
  }
  
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
  
  public void setCellPage(CellPage cellPage) {
    this.cellPage = cellPage;
  }
  
  public String toString() {
    return "CellFilter:columnName=" + columnName + ", cellKey=" + startCellKey + ", time=" + startTimestamp + "," + endTimestamp; 
  }
  
  public long getPagingTimestamp() {
    if(cellPage == null) {
      return 0;
    } else {
      return cellPage.lastTimestamp;
    }
  }

  public boolean isPaging() {
    return cellPage != null;
  }

  public String checkFilterParam() {
    if(cellPage != null) {
      if(numOfVersions > 1) {
        return "Can't use concurrently numOfVersions and CellFilter.setCellPage()";
      }
    } 
    return null;
  }
  
  public static class CellPage implements CWritable {
    private int page;
    private int pageSize;
    private Cell.Key startCellKey;
    private long lastTimestamp;
 
    public CellPage() {
    }
    
    public CellPage(int pageSize) {
      this(pageSize, null, 0);
    }

    public CellPage(int pageSize, Cell.Key previousCellKey) {
      this(pageSize, previousCellKey, 0);
    }
    
    public CellPage(int pageSize, Cell.Key previousCellKey, long lastTimestamp) {
      this.pageSize = pageSize;
      this.lastTimestamp = lastTimestamp;
      
      if(previousCellKey != null && previousCellKey != Cell.Key.EMPTY_KEY) {
        this.startCellKey = new Cell.Key(previousCellKey.nextKeyBytes());
      } else {
        this.startCellKey = Cell.Key.EMPTY_KEY;
      }
    }

    public int getPage() {
      return page;
    }

    public void setPage(int page) {
      this.page = page;
    }

    public int getPageSize() {
      return pageSize;
    }

    public void setPageSize(int pageSize) {
      this.pageSize = pageSize;
    }

    public Cell.Key getStartCellKey() {
      return startCellKey;
    }

    public long getLastTimestamp() {
      return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
      this.lastTimestamp = lastTimestamp;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      page = in.readInt();
      pageSize = in.readInt();
      startCellKey = new Cell.Key();
      startCellKey.readFields(in);
      lastTimestamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(page);
      out.writeInt(pageSize);
      startCellKey.write(out);
      out.writeLong(lastTimestamp);      
    }

  }
}