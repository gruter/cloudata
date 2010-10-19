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
package org.cloudata.examples.first;

import java.io.IOException;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


/**
 * Cloudata에 온라인으로 데이터 입력/조회하는 간단한 프로그램 
 *
 */
public class FirstApp {
  String tableName = "SampleTable";
  CloudataConf conf = new CloudataConf();
  
  public static void main(String[] args) throws IOException {
    FirstApp firstApp = new FirstApp();
    
    //테이블 생성
    firstApp.createTable();
    
    //데이터 저장
    firstApp.putRow();
    
    //timestamep를 고려한 put
    firstApp.putRowTimestamp();
    
    //데이터 조회
    firstApp.getRow();
    
    //scan data
    firstApp.scanning();
    
    //테이블 삭제
    //firstApp.removeRow();
    
    //테이블을 제거한다.
    //firstApp.dropTable();
  }
  
  private void createTable() throws IOException {
    //Column
    String[] columns = new String[]{"Column1", "Column2"};
    
    //테이블 설명
    String description = "테스트";
    
    //TableSchema 객체를 생성한다.
    TableSchema tableSchema = new TableSchema(tableName, description, columns);
    
    //테이블의 MAX 버전 갯수를 지정한다.
    //0: 무한대, default:3
    tableSchema.setNumOfVersion(0);
    
    //기존에 테이블이 존재하는지 확인한다.
    boolean exists = CTable.existsTable(conf, tableName);
    if(exists) {
      System.out.println("Already exists table");
      return;
    }
    //테이블을 생성한다.
    CTable.createTable(conf, tableSchema);
  }
  
  private void putRow() throws IOException {
    //CTable 객체 오픈
    CTable ctable = CTable.openTable(conf, tableName);
    
    //입력할 rowkey      
    Row.Key rowKey = new Row.Key("RowKey1");
    
    //Row 객체 생성
    Row row = new Row(rowKey);
    
    //Cell 추가
    row.addCell("Column1", new Cell(new Cell.Key("CK1"), "TestData1".getBytes()));
    row.addCell("Column2", new Cell(Cell.Key.EMPTY_KEY, "TestData2".getBytes()));
    
    //테이블에 저장
    ctable.put(row);
    
    //새로운 Row 객체 저장
    row = new Row(new Row.Key("RowKey2"));
    row.addCell("Column1", new Cell(new Cell.Key("CK1"), "TestData3".getBytes()));
    
    //테이블에 저장
    ctable.put(row);
  }

  private void putRowTimestamp() throws IOException {
    //CTable 객체 오픈
    CTable ctable = CTable.openTable(conf, tableName);
    
    //입력할 rowkey      
    Row.Key rowKey = new Row.Key("RowKey10");
    
    //Row 객체 생성
    Row row = new Row(rowKey);
    
    //하나의 Row,Cell에 100개의 버전을 저장
    Cell.Key cellKey = new Cell.Key("CK1");
    long timestamp = System.currentTimeMillis();
    for(int i = 0; i < 100; i++) {
      //Cell 추가
      row.addCell("Column1", new Cell(cellKey, String.valueOf(i).getBytes(), timestamp));
      timestamp++;  
    }
    
    //테이블에 저장
    ctable.put(row, false);
  }
  
  private void getRow() throws IOException {
    CTable ctable = CTable.openTable(conf, tableName);
    
    Row row = ctable.get(new Row.Key("RowKey1"));
    
    List<Cell> column1Cells = row.getCellList("Column1");
    System.out.println("--------------- get single row ---------------------");
    for(Cell eachCell: column1Cells) {
      //마지막 버전 값을 가져온다.
      System.out.println(eachCell.getValue().getValueAsString());
    }

    //여러 버전의 데이터를 조회하는 예제
    //putRow()에서 RowKey10에 대해 100개 버전의 데이터를 입력하였다.
    RowFilter rowFilter = new RowFilter(new Row.Key("RowKey10"));
    CellFilter cellFilter = new CellFilter("Column1");
    cellFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);
    rowFilter.addCellFilter(cellFilter);
    
    row = ctable.get(rowFilter);
    Cell cell = row.getCellList("Column1").get(0);
    System.out.println("------- get single row multiple version -----------");
    for(Cell.Value eachValue: cell.getValues()) {
      System.out.println(eachValue.getValueAsString());
    }
    
    //like operation, select * from table where rowkey like 'RowKey%'
    RowFilter likeRowFilter = new RowFilter(new Row.Key("RowKey"), RowFilter.OP_LIKE);
    likeRowFilter.addCellFilter(new CellFilter("Column1"));
    rowFilter.addCellFilter(cellFilter);

    Row[] rows = ctable.gets(likeRowFilter);
    System.out.println("------- get with like operaion -----------");
    if(rows == null || rows.length == 0) {
      System.out.println("No data");
      return;
    } else {
      for(Row eachRow: rows) {
        cell = eachRow.getCellList("Column1").get(0);
        for(Cell.Value eachValue: cell.getValues()) {
          System.out.println(eachValue.getValueAsString());
        }
      }
    }
  }
  
  private void scanning() throws IOException {
    CTable ctable = CTable.openTable(conf, tableName);
    TableScanner scanner = null;
    
    try {
      scanner = ScannerFactory.openScanner(ctable, ctable.getColumnsArray());
      System.out.println("---------- scan ScanCell -----------");
      ScanCell scanCell = null;
      
      while( (scanCell = scanner.next()) != null ) {
        System.out.println(scanCell.toString());
      }
    } finally {
      //must close
      if(scanner != null) {
        scanner.close();
      }
    }
    
    scanner = null;
    
    try {
      scanner = ScannerFactory.openScanner(ctable, ctable.getColumnsArray());
      System.out.println("---------- scan Row -----------");
      Row row = null;
      
      while( (row = scanner.nextRow()) != null ) {
        row.print("Column1");
      }
    } finally {
      //must close
      if(scanner != null) {
        scanner.close();
      }
    }    
  }
  
  private void removeRow() throws IOException {
    CTable ctable = CTable.openTable(conf, tableName);
    ctable.removeRow(new Row.Key("RowKey1"), System.currentTimeMillis());
  }
  
  private void dropTable() throws IOException {
    CTable.dropTable(conf, tableName);    
  }
}
