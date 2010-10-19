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

import org.apache.hadoop.io.Writable;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * Value Object which is returned by TableScanner 
 *
 */
public class ScanCell implements CWritable, Writable, Comparable<ScanCell> {
  private Row.Key rowKey;
  private String columnName;
  
  private Cell.Key cellKey;
  private Cell.Value value;
  
  public ScanCell() {
    
  }
    
  public Row.Key getRowKey() {
    return rowKey;
  }

  public void setRowKey(Row.Key rowKey) {
    this.rowKey = rowKey;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
  
  public void setCellKey(Cell.Key cellKey) {
    this.cellKey = cellKey;
  }
  
  public void setCellValue(Cell.Value value) {
    this.value = value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rowKey = new Row.Key();
    rowKey.readFields(in);
    
    columnName = CWritableUtils.readString(in);

    cellKey = new Cell.Key();
    cellKey.readFields(in);
    
    value = new Cell.Value();
    value.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    rowKey.write(out);
    CWritableUtils.writeString(out, columnName);
    
    cellKey.write(out);
    value.write(out);
  }

  @Override
  public int compareTo(ScanCell obj) {
    if(columnName != null && obj.columnName != null &&
        !columnName.equals(obj.columnName)) {
      return columnName.compareTo(obj.columnName);
    }
    
    if(rowKey == null || rowKey.getBytes() == null)   return -1;
    if(obj.rowKey == null || obj.rowKey.getBytes() == null)   return 1;
    if(!rowKey.equals(obj.rowKey)) {
      return rowKey.compareTo(obj.rowKey);
    }
    
    return cellKey.compareTo(obj.cellKey);
  }
  
  public boolean equals(ScanCell obj) {
    if( !(obj instanceof ScanCell) ) {
      return false;
    }
    
    return compareTo((ScanCell)obj) == 0;
  }  
  
  public Cell.Key getCellKey() {
    return cellKey;
  }

  public byte[] getBytes() {
    if(value == null) {
      return null;
    }
    return value.getBytes();
  }

  public Cell.Value getCellValue() {
    return value;
  }

  public String getPrintValue() {
    if(value == null) {
      return rowKey + "," + columnName + "," + cellKey;
    } else {
      return rowKey + "," + columnName + "," + cellKey + "," + value.getValueAsString();  
    }
  }
  
  public String toString() {
    return getPrintValue();
  }
  
  public static void addCells(Row row, String columnName, List<ScanCell> scanCells) {
    if(scanCells == null || scanCells.size() == 0) {
      return;
    }
    
    Cell.Key previousKey = null;
    
    List<Cell.Value> cellValues = new ArrayList<Cell.Value>();
    for(ScanCell eachCell: scanCells) {
      //System.out.println(">>>>" + eachCell.toString());
      Cell.Key key = eachCell.getCellKey();
      if(previousKey != null && !previousKey.equals(key)) {
        Cell cell = new Cell(previousKey);
        cell.setValues(cellValues);
        row.addCell(columnName, cell);
        cellValues.clear();
      }
      cellValues.add(eachCell.getCellValue());
      previousKey = key;      
    }
    if(cellValues.size() > 0) {
      Cell cell = new Cell(previousKey);
      cell.setValues(cellValues);
      row.addCell(columnName, cell);
      cellValues.clear();
    }   
  }
}
