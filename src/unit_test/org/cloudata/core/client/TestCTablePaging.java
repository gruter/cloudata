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

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.CloudataClusterTestCase;
import org.cloudata.core.tablet.TableSchema;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * @author jindolk
 *
 */
public class TestCTablePaging extends CloudataClusterTestCase {
  private String pagingTableName = "T_NTABLE_PAGING";

  private String versionPagingTableName = "T_NTABLE_VERSION_PAGING";

  private int ROW_NUM = 10240;
  private int VERSION_ROW_NUM = 100;
  private int CK_NUM = 20;
  private int VERSION_NUM = 8;
  //private NConfiguration conf = new NConfiguration();
  private DecimalFormat df = new DecimalFormat("0000000000");
  
  public TestCTablePaging(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    //conf = new NConfiguration();
    super.setUp();
  }
  
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  private void insertPagingTestData() throws Exception {
    if(!CTable.existsTable(conf, pagingTableName)) {
      //NTable.dropTable(conf, pagingTableName);
      TableSchema tableSchema = new TableSchema(pagingTableName);
      tableSchema.addColumn("Column1");
      
      CTable.createTable(conf, tableSchema);
    } else {
      return;
    }
    
    CTable ctable = CTable.openTable(conf, pagingTableName);

    Random rand = new Random();
    
    byte[] data = new byte[10000];
    
    for(int i = 0; i < ROW_NUM; i++) {
      Row.Key rowKey = new Row.Key(df.format(i + 1));
      
      for(int j = 0; j < CK_NUM; j++) {
        Row row = new Row(rowKey);
        rand.nextBytes(data);
        Cell.Key cellKey = new Cell.Key(df.format(j + 1));
        row.addCell("Column1", new Cell(cellKey, data));
        ctable.put(row);
      }        
    }
  }
  
  private void insertCellPagingVersionData() throws Exception {
    if(!CTable.existsTable(conf, versionPagingTableName)) {
      CTable.dropTable(conf, versionPagingTableName);
      TableSchema tableSchema = new TableSchema(versionPagingTableName);
      tableSchema.addColumn("Column1");
      tableSchema.setNumOfVersion(0);
      
      CTable.createTable(conf, tableSchema);
    } else {
      return;
    }
    
    CTable ctable = CTable.openTable(conf, versionPagingTableName);

    Random rand = new Random();
    
    byte[] data = new byte[10000];
    
    for(int i = 0; i < VERSION_ROW_NUM; i++) {
      Row.Key rowKey = new Row.Key(df.format(i + 1));
      
      for(int j = 0; j < CK_NUM; j++) {
        Row row = new Row(rowKey);
        rand.nextBytes(data);
        Cell.Key cellKey = new Cell.Key(df.format(j + 1));
        for(int k = 0; k < VERSION_NUM; k++) {
          row.addCell("Column1", new Cell(cellKey, data, k + 1));
        }
        ctable.put(row, false);
      }        
    }
  }
  
  public void testCellPaging() throws Exception {
    System.out.println("=================================== testCellPaging ===================================");
    insertPagingTestData();
    
    int pageSize = 10;
    int lastPage = (CK_NUM / pageSize) + 1;
    
    CTable ctable = CTable.openTable(conf, pagingTableName);
    
    RowFilter rowFilter = new RowFilter(new Row.Key(df.format(2)));
    
    CellFilter cellFilter = new CellFilter("Column1");
    cellFilter.setCellPage(new CellFilter.CellPage(pageSize));
    rowFilter.addCellFilter(cellFilter);
    
    int page = 1;
    while(true) {
      Row row = ctable.get(rowFilter);
      
      if(row == null) {
        break;
      }
      
      List<Cell> cells = row.getCellList("Column1");
      if(cells == null || cells.size() == 0) {
        break;
      }
      
      System.out.println("--------------" + page + " page datas--------------");
      int valueCount = 0;
      for(Cell eachCell: cells) {
        for(Cell.Value eachValue: eachCell.values) {
          //System.out.println(eachCell.getKey() + ":" + eachValue.getTimestamp());
          
          assertEquals("Cell.Key equals ", new Cell.Key(df.format((page - 1) * pageSize + (valueCount + 1))), eachCell.getKey());
          valueCount++;
        }
      }

      //assert
      if(page == lastPage) {
        int lastDataCount = CK_NUM % pageSize;
        assertEquals("number of datas in last page", lastDataCount, valueCount);
      } else {
        assertEquals("number of datas in the page", pageSize, valueCount);
      }
      
      cellFilter.setCellPage(row.getNextCellPage("Column1", pageSize));
      page++;
    }
    
    assertEquals(lastPage, page - 1);
  }
  
  public void testCellVersionPaging() throws Exception {
    System.out.println("=================================== testCellVersionPaging ===================================");
    insertCellPagingVersionData();
    
    int pageSize = 10;
    int lastPage = (CK_NUM / pageSize) + 1;
    
    CTable ctable = CTable.openTable(conf, versionPagingTableName);
    
    RowFilter rowFilter = new RowFilter(new Row.Key(df.format(45)));
    
    CellFilter cellFilter = new CellFilter("Column1");
    cellFilter.setCellPage(new CellFilter.CellPage(pageSize));
    rowFilter.addCellFilter(cellFilter);
    
    int page = 1;
    while(true) {
      Row row = ctable.get(rowFilter);
      
      if(row == null) {
        break;
      }
      
      List<Cell> cells = row.getCellList("Column1");
      if(cells == null || cells.size() == 0) {
        break;
      }
      
      System.out.println("--------------" + page + " page datas--------------");
      int valueCount = 0;
      for(Cell eachCell: cells) {
        for(Cell.Value eachValue: eachCell.values) {
          System.out.println(eachCell.getKey() + ":" + eachValue.getTimestamp());
          assertEquals("Cell.Key equals ", new Cell.Key(df.format((page - 1) * pageSize + (valueCount + 1))), eachCell.getKey());
          assertEquals("lastest version", VERSION_NUM, eachValue.getTimestamp());
          valueCount++;
        }
      }

      //assert
      if(page == lastPage) {
        int lastDataCount = (CK_NUM) % pageSize;
        assertEquals("number of datas in last page", lastDataCount, valueCount);
      } else {
        assertEquals("number of datas in the page", pageSize, valueCount);
      }
      
      cellFilter.setCellPage(row.getNextCellPage("Column1", pageSize));
      page++;
    }

    rowFilter = new RowFilter(new Row.Key(df.format(45)));
    
    cellFilter = new CellFilter("Column1");
    cellFilter.setCellPage(new CellFilter.CellPage(pageSize));
    cellFilter.setNumOfVersions(VERSION_NUM);
    rowFilter.addCellFilter(cellFilter);
    
    try {
      ctable.get(rowFilter);
      fail("Can't use concurrently numOfVersions and CellFilter.setCellPage()");
    } catch (IOException e) {
      
    }
  }
  
  /**
   * like, between에서와 같이 multi-row 연산에서 paging 처리 기능
   * @throws Exception
   */
  public void testRowPaging() throws Exception {
    System.out.println("=================================== testRowPaging ===================================");
    int pageSize = 10;
    RowFilter rowFilter = new RowFilter(new Row.Key("0000"), RowFilter.OP_LIKE);
    rowFilter.setPagingInfo(pageSize, null);
    
    rowFilter.addCellFilter(new CellFilter("Column1"));
    
    CTable ctable = CTable.openTable(conf, pagingTableName);
    
    int page = 1;
    int lastPage = (ROW_NUM / pageSize) + 1;

    long elapseTime = 0;

    while(true) {
      long time = 0;
      long startTime = System.currentTimeMillis();
      Row[] rows = ctable.gets(rowFilter);
      time = (System.currentTimeMillis() - startTime);;
      elapseTime += time;
      
      if(rows == null || rows.length == 0) {
        break;
      }

      System.out.println("--------------" + page + " page datas--------------:" + time);
      int index = 0;
      for(Row eachRow: rows) {
        Row.Key rowKey = eachRow.getKey();
        assertEquals(new Row.Key((df.format((page - 1) * pageSize + (index + 1)))), rowKey);
        index++;
      }
      
      //assert
      if(page == lastPage) {
        int lastDataCount = ROW_NUM % pageSize;
        assertEquals("number of datas in last page", lastDataCount, rows.length);
      } else {
        assertEquals("number of datas in the page", pageSize, rows.length);
      }
      
      page++;
      
      rowFilter.setPagingInfo(pageSize, rows[rows.length - 1].getKey());
    }

    System.out.println("time: " + elapseTime + ", " + (elapseTime/(page-1)));

    assertEquals(lastPage, page - 1);
  }
  
  public void testPagingWhileCompaction() throws IOException {
    System.out.println("=================================== testPagingWhileCompaction ===================================");
    String tableName = "T_PAGING_TEST";
    if(CTable.existsTable(conf, tableName)) {
      CTable.dropTable(conf, tableName);
    }
    TableSchema tableSchema = new TableSchema(tableName);
    tableSchema.addColumn("Column1");
    
    CTable.createTable(conf, tableSchema);
    
    PagingInsertThread insertThread = new PagingInsertThread(tableName);
    insertThread.start();
    
    PagingGetThread readThread1 = new PagingGetThread(tableName, insertThread);
    readThread1.start();

    PagingGetThread readThread2 = new PagingGetThread(tableName, insertThread);
    readThread2.start();

    while(true) {
      if(!insertThread.isAlive()) {
        readThread1.setStop(true);
        readThread2.setStop(true);
        break;
      }
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        return;
      }
    }
  }
  
  class PagingInsertThread extends Thread {
    String tableName;
    int totalInsertCount;
    
    public PagingInsertThread(String tableName) {
      this.tableName = tableName;
    }
    
    public void run() {
      try {
        insert();
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
    
    private void insert() throws IOException {
      CTable ctable = CTable.openTable(conf, tableName);
  
      Random rand = new Random();
      
      byte[] data = new byte[10000];
      
      for(int i = 0; i < 10 * 1000; i++) {
        Row.Key rowKey = new Row.Key(df.format(i + 1));
        
        for(int j = 0; j < 10; j++) {
          Row row = new Row(rowKey);
          rand.nextBytes(data);
          Cell.Key cellKey = new Cell.Key(df.format(j + 1));
          row.addCell("Column1", new Cell(cellKey, data));
          ctable.put(row);
        }     
        totalInsertCount++;
        
        if(totalInsertCount % 1000 == 0) {
          System.out.println(totalInsertCount + " inserted");
        }
      }
    }
  }
  
  class PagingGetThread extends Thread {
    String tableName;
    boolean stop = false;
    PagingInsertThread insertThread;
    public PagingGetThread(String tableName, PagingInsertThread insertThread) {
      this.tableName = tableName;
      this.insertThread = insertThread;
    }
    
    public void run() {
      try {
        int loopCount = 0;
        while(!stop) {
          int insertedCount = insertThread.totalInsertCount;
          
          int readedCount = getDatas();
          
          assertTrue(readedCount >= insertedCount);
          
          loopCount++;
          
          if(loopCount % 100 == 0) {
            System.out.println(loopCount + " asserted");
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
    
    private void setStop(boolean stop) {
      this.stop = stop;
    }
    
    private int getDatas() throws IOException {
      int pageSize = 10;
      RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
      rowFilter.setPagingInfo(pageSize, null);
      
      rowFilter.addCellFilter(new CellFilter("Column1"));
      
      CTable ctable = CTable.openTable(conf, tableName);
      
      int totalCount = 0;
      while(true) {
        Row[] rows = ctable.gets(rowFilter);
        
        if(rows == null || rows.length == 0) {
          break;
        }

        for(Row eachRow: rows) {
          Row.Key rowKey = eachRow.getKey();
          totalCount++;
        }
        
        rowFilter.setPagingInfo(pageSize, rows[rows.length - 1].getKey());
      }
      
      return totalCount;
    }
  }
  
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestCTablePaging("testCellPaging"));
    suite.addTest(new TestCTablePaging("testRowPaging"));
    suite.addTest(new TestCTablePaging("testCellVersionPaging"));
    //suite.addTest(new TestNTablePaging("testPagingWhileCompaction"));
    return suite;
  }
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestCTablePaging.suite());
    //(new TestNTablePaging("Test")).insertPagingTestData();
  }
}
