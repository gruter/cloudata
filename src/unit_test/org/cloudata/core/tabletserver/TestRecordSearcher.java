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
import java.text.DecimalFormat;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.CellFilter.CellPage;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.DiskSSTable;
import org.cloudata.core.tabletserver.MemorySSTable;
import org.cloudata.core.tabletserver.RecordSearcher;
import org.cloudata.core.tabletserver.ServerSideMultiColumnScanner;
import org.cloudata.core.tabletserver.TabletScanner;

import junit.framework.TestCase;


/**
 * @author jindolk
 *
 */
public abstract class TestRecordSearcher extends TestCase {
  public static boolean PRINT = false;
  protected CloudataConf conf = new CloudataConf();
  protected DecimalFormat df = new DecimalFormat("0000000000");
  protected int NUM_ROWS = 100;
  protected int NUM_CELL1 = 3;
  protected int NUM_CELL2 = 20;
  
  protected int TIMESTAMP1 = 10;
  protected int TIMESTAMP2 = 100;
  
  protected TabletInfo tabletInfo = new TabletInfo("T_TEST", "T_TEST_1234", Row.Key.MIN_KEY, Row.Key.MAX_KEY);

  protected MemorySSTable memorySSTable;
  protected DiskSSTable diskSSTable;
  
  protected abstract MemorySSTable makeMemorySSTable() throws IOException;
  protected abstract DiskSSTable makeDiskSSTable() throws IOException;
  
  public TestRecordSearcher(String name) {
    super(name);
    conf.set("cloudata.filesystem", "local");
  }
  
  public void setUp() throws IOException {
    memorySSTable = makeMemorySSTable();
    diskSSTable = makeDiskSSTable();
  }
  
  protected int getNumCells(int rowId) {
    return (rowId % 2 == 0 ? NUM_CELL1 : NUM_CELL2);
  }
  
  public void testFilter() throws IOException {
    System.out.println("===================================== testRowFilter =====================================");

    RecordSearcher searcher = new RecordSearcher(memorySSTable, diskSSTable, 3);
    //1. get with row key
    for(int i = 0; i < NUM_ROWS; i++) {
      Row.Key rowKey = new Row.Key(df.format(i));
      
      ColumnValue[] columnValues = searcher.search(rowKey, "Col1");
      
      assertNotNull(columnValues);
      assertTrue(columnValues.length > 0);
      
      Row row = RowColumnValues.makeRow(
          new RowColumnValues[]{new RowColumnValues("Col1", rowKey, columnValues)});
      
      assertEquals(rowKey, row.getKey());
      List<Cell> cells = row.getCellList("Col1");
      
      assertEquals(getNumCells(i), cells.size());

      columnValues = searcher.search(rowKey, "Col2");
      assertNotNull(columnValues);
      assertEquals(1, columnValues.length);
    }
    
    //2. get with cell key
    for(int i = 0; i < NUM_ROWS; i++) {
      Row.Key rowKey = new Row.Key(df.format(i));
      
      int numCells = getNumCells(i);
      for(int j = 0; j < numCells; j++) {
        Cell.Key cellKey = new Cell.Key(df.format(j));
        ColumnValue columnValue = searcher.search(rowKey, "Col1", cellKey);
        assertNotNull(columnValue);
        
        assertEquals(rowKey, columnValue.getRowKey());
        assertEquals(cellKey, columnValue.getCellKey());
      }
      ColumnValue columnValue = searcher.search(rowKey, "Col2", Cell.Key.EMPTY_KEY);
      assertNotNull(columnValue);
      assertEquals(rowKey, columnValue.getRowKey());
      assertEquals(Cell.Key.EMPTY_KEY, columnValue.getCellKey());
    }
    
    //3. get with CellFilter(timestamp)
    for(int i = 0; i < NUM_ROWS; i++) {
      RowFilter rowFilter = new RowFilter(new Row.Key(df.format(i)));
      CellFilter cellFilter1 = new CellFilter("Col1");
      cellFilter1.setTimestamp(TIMESTAMP1, TIMESTAMP1 + 2);
      rowFilter.addCellFilter(cellFilter1);
      
      RowColumnValues[] values = searcher.search(rowFilter);
      if(i % 2 == 1) {
        assertNull(values);
        continue;
      } 
      assertNotNull(values);
      Row row = RowColumnValues.makeRow(values);
      
      List<Cell> cells = row.getCellList("Col1"); 
      assertEquals(2, cells.size());
    }
    
    //4. get with CellFilter(numOfVersions)
    for(int i = 0; i < NUM_ROWS; i++) {
      RowFilter rowFilter = new RowFilter(new Row.Key(df.format(i)));
      CellFilter cellFilter1 = new CellFilter("Col1");
      cellFilter1.setNumOfVersions(2);
      rowFilter.addCellFilter(cellFilter1);
      
      RowColumnValues[] values = searcher.search(rowFilter);
      assertNotNull(values);
      Row row = RowColumnValues.makeRow(values);
      
      List<Cell> cells = row.getCellList("Col1"); 
      assertEquals(getNumCells(i), cells.size());
      for(Cell eachCell: cells) {
        List<Cell.Value> cellValues = eachCell.getValues();
        assertEquals(2, cellValues.size());
      }
    }

    //5. get with CellFilter(cellPaging)
    int pageSize = 5;
    for(int i = 0; i < NUM_ROWS; i++) {
      int readCell = 0;
      int startKeyIndex = 2;
      RowFilter rowFilter = new RowFilter(new Row.Key(df.format(i)));
      CellFilter cellFilter1 = new CellFilter("Col1", new Cell.Key(df.format(startKeyIndex)), new Cell.Key(df.format(15)));
      cellFilter1.setCellPage(new CellPage(pageSize));
      rowFilter.addCellFilter(cellFilter1);

      RowColumnValues[] values = null;
      int page = 1;
      while( (values = searcher.search(rowFilter)) != null ) {
        assertNotNull(values);
        Row row = RowColumnValues.makeRow(values);
        
        
        int expectedCells = 0;
        if(i % 2 == 0) {
          expectedCells = 1;
        } else {
          if(page == 3) {
            expectedCells = 4; 
          } else {
            expectedCells = pageSize;
          }
        }
        
        List<Cell> cells = row.getCellList("Col1"); 
        assertEquals(expectedCells, cells.size());
        
        int index = 0;
        for(Cell eachCell: cells) {
          assertEquals(new Cell.Key(df.format(startKeyIndex + (page - 1) * pageSize + index)), eachCell.getKey());
          readCell++;
          index++;
        }
        page++;
        cellFilter1.setCellPage(row.getNextCellPage("Col1", pageSize));
      }
      assertEquals(page - 1, i % 2 == 0 ? 1: 3);
      assertEquals(readCell, i % 2 == 0 ? 1: 14);
    }

    //6. get with CellFilter(startCellKey, endCellKey)
    int startCellIndex = 1;
    int endCellIndex = 9;
    for(int i = 0; i < NUM_ROWS; i++) {
      RowFilter rowFilter = new RowFilter(new Row.Key(df.format(i)));
      CellFilter cellFilter1 = new CellFilter("Col1", new Cell.Key(df.format(startCellIndex)),new Cell.Key(df.format(endCellIndex)));
      CellFilter cellFilter2 = new CellFilter("Col2", new Cell.Key(df.format(startCellIndex)),new Cell.Key(df.format(endCellIndex)));
      rowFilter.addCellFilter(cellFilter1);
      rowFilter.addCellFilter(cellFilter2);
      RowColumnValues[] values = searcher.search(rowFilter);
      
      assertNotNull(values);
      
      int expectedCells = (i % 2 == 0 ? 2 : 9);
      Row row = RowColumnValues.makeRow(values);
      List<Cell> cells = row.getCellList("Col1"); 
      
      assertEquals(i + " row" , expectedCells, cells.size());
      
      for(int j = 0; j < expectedCells; j++) {
        Cell cell = cells.get(j);
        assertEquals(df.format(startCellIndex + j), cell.getKey().toString());
      }
      
      cells = row.getCellList("Col2");
      assertNull(cells);
    }
    
    startCellIndex = 5;
    endCellIndex = 9;
    for(int i = 0; i < NUM_ROWS; i++) {
      RowFilter rowFilter = new RowFilter(new Row.Key(df.format(i)));
      CellFilter cellFilter1 = new CellFilter("Col1", new Cell.Key(df.format(startCellIndex)),new Cell.Key(df.format(endCellIndex)));
      CellFilter cellFilter2 = new CellFilter("Col2", new Cell.Key(df.format(startCellIndex)),new Cell.Key(df.format(endCellIndex)));
      rowFilter.addCellFilter(cellFilter1);
      rowFilter.addCellFilter(cellFilter2);
      RowColumnValues[] values = searcher.search(rowFilter);
      
      if(i % 2 == 0) {
        assertNull(values);
        continue;
      }
      assertNotNull(values);
      int expectedCells = 5;
      Row row = RowColumnValues.makeRow(values);
      List<Cell> cells = row.getCellList("Col1"); 
      
      assertEquals(i + " row" , expectedCells, cells.size());
      
      for(int j = 0; j < expectedCells; j++) {
        Cell cell = cells.get(j);
        assertEquals(df.format(startCellIndex + j), cell.getKey().toString());
      }
      
      cells = row.getCellList("Col2");
      assertNull(cells);
    }
    
    //7. get with CellFilter(startCellKey, endCellKey, numOfVersion)
    startCellIndex = 1;
    endCellIndex = 9;
    for(int i = 0; i < NUM_ROWS; i++) {
      RowFilter rowFilter = new RowFilter(new Row.Key(df.format(i)));
      CellFilter cellFilter1 = new CellFilter("Col1", new Cell.Key(df.format(startCellIndex)),new Cell.Key(df.format(endCellIndex)));
      cellFilter1.setNumOfVersions(2);
      rowFilter.addCellFilter(cellFilter1);
      
      RowColumnValues[] values = searcher.search(rowFilter);
      
      assertNotNull(values);
      
      int expectedCells = (i % 2 == 0 ? 2 : 9);
      Row row = RowColumnValues.makeRow(values);
      List<Cell> cells = row.getCellList("Col1"); 
      
      assertEquals(i + " row" , expectedCells, cells.size());
      
      for(int j = 0; j < expectedCells; j++) {
        Cell cell = cells.get(j);
        assertEquals(df.format(startCellIndex + j), cell.getKey().toString());
        
        List<Cell.Value> cellValues = cell.getValues();
        assertNotNull(cellValues);
        assertEquals(2, cellValues.size());
        
        int index = 1;
        for(Cell.Value cellValue: cellValues) {
          assertEquals(df.format(i) + "_" + df.format(startCellIndex + j) + "_Cell1DataV" + index, cellValue.getValueAsString());
          index--;
        }
      }
    }
    
    //8. get with CellFilter(timestamp)
    for(int i = 0; i < NUM_ROWS; i++) {
      RowFilter rowFilter = new RowFilter(new Row.Key(df.format(i)), new Row.Key(df.format(i)),
          RowFilter.OP_EQ);

      CellFilter cellFilter = new CellFilter("Col2");
      cellFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);
      rowFilter.addCellFilter(cellFilter);
      
      RowColumnValues[] values = searcher.search(rowFilter);
      assertNotNull(values);
      
      Row row = RowColumnValues.makeRow(values);
      List<Cell> cells = row.getCellList("Col2");
      assertNotNull(cells);
      assertEquals(1, cells.size());
    }
  }
  
  public void testScanner() throws IOException {
    System.out.println("===================================== testScanner =====================================");
    
    //1. Scan all data
    RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY);
    CellFilter cellFilter = new CellFilter("Col1");
    rowFilter.addCellFilter(cellFilter);

    TabletScanner tabletScanner = new TabletScanner("test", conf, tabletInfo, 
        rowFilter.getStartRowKey(), rowFilter.getEndRowKey(), "Col1", memorySSTable, diskSSTable, 
        cellFilter, 3, false);
    ServerSideMultiColumnScanner scanner = new ServerSideMultiColumnScanner(null, rowFilter, new TabletScanner[]{tabletScanner});
    
    RowColumnValues[] rowColumnValue = null;
    
    int rowCount = 0;
    
    while((rowColumnValue = scanner.nextRow()) != null) {
      Row row = RowColumnValues.makeRow(rowColumnValue);
      
      assertEquals(new Row.Key(df.format(rowCount)), row.getKey());
      List<Cell> cells = row.getCellList("Col1");
      assertNotNull(cells);
      assertEquals(getNumCells(rowCount), cells.size());
      rowCount++;
    }
    scanner.close();
    assertEquals(rowCount, NUM_ROWS);
    
    //2. Range Scan
    rowFilter = new RowFilter(new Row.Key(df.format(10)), new Row.Key(df.format(30)));
    cellFilter = new CellFilter("Col1");
    rowFilter.addCellFilter(cellFilter);

    tabletScanner = new TabletScanner("test", conf, tabletInfo, 
        rowFilter.getStartRowKey(), rowFilter.getEndRowKey(), "Col1", memorySSTable, diskSSTable, 
        cellFilter, 3, false);
    scanner = new ServerSideMultiColumnScanner(null, rowFilter, new TabletScanner[]{tabletScanner});
    
    rowColumnValue = null;
    
    rowCount = 0;
    int rowIndex = 10;
    while((rowColumnValue = scanner.nextRow()) != null) {
      Row row = RowColumnValues.makeRow(rowColumnValue);
      
      assertEquals(new Row.Key(df.format(rowIndex)), row.getKey());
      List<Cell> cells = row.getCellList("Col1");
      assertNotNull(cells);
      assertEquals(getNumCells(rowCount), cells.size());
      rowCount++;
      rowIndex++;
    }
    scanner.close();
    assertEquals(rowCount, 21);
  }
}
