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
package org.cloudata.core.client.procedure;

import java.io.IOException;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


public class Selector {
  CTable ctable;
  TableSchema tableSchema;
  String[] columns;
  Row.Key startRowKey = Row.Key.MIN_KEY;
  Row.Key endRowKey = Row.Key.MAX_KEY;
//  SelectQuery query;
  CloudataConf conf ;
  
  public Selector(String sql) throws IOException {
//    try {
//      query = new SelectQuery(sql);
//      query.parse();
//    } catch (ParseException e) {
//      throw new IOException(e.getMessage());
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//    //this.conf = new CloudataConf();
//    this.ctable = CTable.openTable(Shell.conf, query.getFromTable());
//    if (this.ctable == null) {
//      throw new IOException("Table [" + query.getFromTable() + "] not found.");
//    }
//    this.tableSchema = ctable.descTable();
//    initColumns();
//    
//    if (query.getRowKey() != null) {
//      this.startRowKey = query.getRowKey();
//      this.endRowKey = query.getRowKey();
//    }
  }

  /**
   * query에 RowKey가 있는 경우에는 scanner를 이용하지 않고 get을 이용하여 조회한다.
   * @return
   * @throws IOException
   */
  public Row querySingleRow() throws IOException {
//    if(query.getRowKey() == null) {
//      throw new IOException("No rowkey in query(select * from table_name where rowkey='rk1')");
//    }
//    
//    if(columns == null || columns.length == 0) {
//      throw new IOException("No columns");
//    }
//    RowFilter rowFilter = new RowFilter(query.getRowKey());
//    for(String eachColumn: columns) {
//      CellFilter cellFilter = new CellFilter(eachColumn);
//      cellFilter.setStartTimestamp(Long.MIN_VALUE);
//      cellFilter.setEndTimestamp(Long.MAX_VALUE);
//      rowFilter.addCellFilter(cellFilter);
//    }
//    return ctable.get(rowFilter);
    
    return null;
  }
  
  public String[] getColumns() {
    return columns;
  }
  
  public int getColumnIndex(String columnName) {
    for(int i = 0; i < columns.length; i++) {
      if(columns[i].equals(columnName)) {
        return i;
      }
    }
    
    return -1;
  }
  
  private void initColumns() throws IOException {
//    boolean all = false;
//    for (String eachColumn : query.getColumns()) {
//      if ("*".equals(eachColumn)) {
//        all = true;
//        break;
//      }
//    }
//
//    if (all) {
//      columns = tableSchema.getColumnsArray();
//    } else {
//      columns = query.getColumns().toArray(new String[] {});
//      if (!tableSchema.containsColumn(columns)) {
//        throw new IOException("Invalid column name");
//      }
//    }
  }
  
  public TableScanner getScanner() throws IOException {
//    // open scanner
//    RowFilter scanFilter = new RowFilter(startRowKey, endRowKey);
//    //System.out.println("Time:" + query.getStartTime() + "," + query.getEndTime());
//    for(String eachColumn: columns) {
//      CellFilter cellFilter = new CellFilter(eachColumn);
//      cellFilter.setStartTimestamp(query.getStartTime());
//      cellFilter.setEndTimestamp(query.getEndTime());
//      
//      scanFilter.addCellFilter(cellFilter);
//    }
//    
//    return ScannerFactory.openScanner(
//        ctable, 
//        scanFilter,
//        20);
    return null;
  }
  
  public Row select() throws IOException {
//    if(query.getCellKeys().size() > 0) {
//      Map<String, Cell.Key> cellKeys = query.getCellKeys();
//      ColumnValue[][] result = new ColumnValue[columnKeys.size()][];
//      for(int i = 0; i < columns.length; i++) {
//        result[i] = new ColumnValue[]{
//            new ColumnValue(query.getRowKey(),
//                    columnKeys.get(columns[i]),
//                    ctable.get(query.getRowKey(), columns[0], columnKeys.get(columns[i])))
//            };
//      }
//      return result;
//    } else if(query.getRowKey() != null) {
//      return ctable.get(query.getRowKey(), columns);
//    } else {
//      TableScanner scanner = getScanner();
//      
//      // results의 각 배열에는 하나의 Row.Key에 대한 컬럼별로 컬럼데이터가 저장되어 있다.
//      List[] valueList = new List[columns.length];
//  
//      for(int i = 0; i < columns.length; i++) {
//        valueList[i] = new ArrayList<Cell>();
//      }
//      
//      Row row = null;
//      while ((row = scanner.nextRow()) != null) {
//        for (int i = 0; i < columns.length; i++) {
//          if (row.getCells(columns[i]) != null) {
//            List<Cell> cells = row.getCells(columns[i]);
//            valueList[i].add(cells);
//          }
//        }
//      } // end of while
//      
//      ColumnValue[][] result = new ColumnValue[columns.length][];
//      for(int i = 0; i < columns.length; i++) {
//        result[i] = new ColumnValue[valueList[i].size()];
//        valueList[i].toArray(result[i]);
//      }
//      return result;
//    }
    
    return null;
  }
}
