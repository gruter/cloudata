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
package org.cloudata.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;
import org.cloudata.thrift.generated.ThriftCell;
import org.cloudata.thrift.generated.ThriftCellValue;
import org.cloudata.thrift.generated.ThriftColumnInfo;
import org.cloudata.thrift.generated.ThriftRow;
import org.cloudata.thrift.generated.ThriftTableSchema;


/**
 * @author jindolk
 *
 */
public class ThriftUtil {
  public static Row getNRow(ThriftRow tRow) {
    Row row = new Row(new Row.Key(tRow.getRowKey()));
    
    Map<String, TreeMap<Cell.Key, Cell>> cells = new TreeMap<String, TreeMap<Cell.Key, Cell>>();
    
    for(Map.Entry<String, List<ThriftCell>> entry: tRow.getCells().entrySet()) {
      cells.put(entry.getKey(), getNCells(entry.getValue()));
    }
    
    row.setAllCells(cells);
    return row;
  }
  
  public static TreeMap<Cell.Key, Cell> getNCells(List<ThriftCell> tCells) {
    TreeMap<Cell.Key, Cell> cells = new TreeMap<Cell.Key, Cell>();
    
    for(ThriftCell eachCell: tCells) {
      Cell cell = getNCell(eachCell);
      if(cells.containsKey(cell.getKey())) {
        Cell aCell = cells.get(cell.getKey());
        if(cell.getValues() != null) {
          for(Cell.Value eachCellValue: cell.getValues()) {
            aCell.addValue(eachCellValue);
          }
        }
      } else {
        cells.put(cell.getKey(), cell);
      }
    }
    return cells;
  }
  
  public static Cell getNCell(ThriftCell tCell) {
    Cell cell = new Cell();
    
    cell.setKey(new Cell.Key(tCell.getCellKey()));
   
    if(tCell.getValues() != null) {
      List<Cell.Value> values = new ArrayList<Cell.Value>();
      for(ThriftCellValue eachValue: tCell.getValues()) {
        values.add(getNCellValue(eachValue));
      }
      
      cell.setValues(values);
    }
    
    return cell;
  }
  
  public static Cell.Value getNCellValue(ThriftCellValue tValue) {
    Cell.Value cellValue = new Cell.Value();
    
    cellValue.setBytes(tValue.getData());
    cellValue.setDeleted(tValue.isDeleted());
    cellValue.setTimestamp(Long.parseLong(tValue.getTimestamp()));
    
    return cellValue;
  }
  
  public static ThriftRow getTRow(Row row) {
    ThriftRow tRow = new ThriftRow();
    
    tRow.setRowKey(row.getKey().toString());
    
    Map<String, List<ThriftCell>> cells = new HashMap<String, List<ThriftCell>>();
    
    for(String eachColumnName: row.getColumnNames()) {
      cells.put(eachColumnName, getTCells(row.getCellList(eachColumnName)));
    }
    tRow.setCells(cells);
    return tRow;
  }
  
  public static List<ThriftCell> getTCells(List<Cell> cells) {
    List<ThriftCell> tCells = new ArrayList<ThriftCell>();
    
    for(Cell eachCell: cells) {
      tCells.add(getTCell(eachCell));
    }
    return tCells;
  }
  
  public static ThriftCell getTCell(Cell cell) {
    ThriftCell tCell = new ThriftCell();
    
    tCell.setCellKey(cell.getKey().toString());
   
    if(cell.getValues() != null) {
      List<ThriftCellValue> tValues = new ArrayList<ThriftCellValue>();
      for(Cell.Value eachValue: cell.getValues()) {
        tValues.add(getTCellValue(eachValue));
      }
      
      tCell.setValues(tValues);
    }
    
    return tCell;
  }
  
  public static ThriftCellValue getTCellValue(Cell.Value value) {
    ThriftCellValue tCellValue = new ThriftCellValue();
    
    tCellValue.setData(value.getBytes());
    tCellValue.setDeleted(value.isDeleted());
    tCellValue.setTimestamp("" + value.getTimestamp());
    
    return tCellValue;
  }
  
  public static TableSchema getNTableSchema(ThriftTableSchema thriftSchema) {
    TableSchema nTableSchema = new TableSchema(thriftSchema.getTableName());
    
    nTableSchema.setOwner(thriftSchema.getOwner());
    nTableSchema.setDescription(thriftSchema.getDescription());
    if(thriftSchema.getNumOfVersion() == null) {
      thriftSchema.setNumOfVersion("3");
    } else {
      nTableSchema.setNumOfVersion(Integer.parseInt(thriftSchema.getNumOfVersion()));
    }
    
    for(ThriftColumnInfo column: thriftSchema.getColumns()) {
      nTableSchema.addColumn(getNColumnInfo(column));
    }
    
    return nTableSchema;
  }

  public static ThriftTableSchema getTTableSchema(TableSchema nTableSchema) {
    ThriftTableSchema tTableSchema = new ThriftTableSchema();

    tTableSchema.setTableName(nTableSchema.getTableName());
    tTableSchema.setOwner(nTableSchema.getOwner());
    tTableSchema.setDescription(nTableSchema.getDescription());
    tTableSchema.setNumOfVersion("" + nTableSchema.getNumOfVersion());
    
    for(ColumnInfo column: nTableSchema.getColumnInfos()) {
      tTableSchema.addToColumns(getTColumnInfo(column));
    }
    
    return tTableSchema;
  }

  public static ColumnInfo getNColumnInfo(ThriftColumnInfo thriftColumn) {
    return new ColumnInfo(thriftColumn.getColumnName(), Integer.parseInt(thriftColumn.getColumnType()));
  }

  public static ThriftColumnInfo getTColumnInfo(ColumnInfo column) {
    ThriftColumnInfo tColumnInfo = new ThriftColumnInfo();
    tColumnInfo.setColumnName(column.getColumnName());
    tColumnInfo.setColumnType("" + column.getColumnType());
    return tColumnInfo;
  }
}
