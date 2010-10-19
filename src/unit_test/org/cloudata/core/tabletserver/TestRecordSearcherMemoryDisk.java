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
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;
import org.cloudata.core.tabletserver.ColumnCollection;
import org.cloudata.core.tabletserver.DiskSSTable;
import org.cloudata.core.tabletserver.MemorySSTable;
import org.cloudata.core.tabletserver.RecordSearcher;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileWriter;


/**
 * @author jindolk
 *
 */
public class TestRecordSearcherMemoryDisk extends TestRecordSearcher {
  public TestRecordSearcherMemoryDisk(String name) throws IOException {
    super(name);
  }

  @Override
  protected MemorySSTable makeMemorySSTable() throws IOException {
    MemorySSTable memorySSTable = new MemorySSTable();
    Tablet tablet = new Tablet(conf, null, null, tabletInfo);
    memorySSTable.init(null, conf, tablet, 3);
    
    ColumnCollection columnCollectionCol1 = new ColumnCollection(conf);
    ColumnCollection columnCollectionCol2 = new ColumnCollection(conf);
    
    for(int i = 0; i < NUM_ROWS; i++) {
      if(i % 3 != 0) {
        continue;
      }
      Row.Key rowKey = new Row.Key(df.format(i));
      
      int numCells = getNumCells(i);
      long timestamp = (i % 2 == 0 ? TIMESTAMP1: TIMESTAMP2);
      for(int j = 0; j < numCells; j++) {
        for(int k = 0; k < 2; k++) {
          ColumnValue columnValue = new ColumnValue();
          columnValue.setRowKey(rowKey);
          columnValue.setCellKey(new Cell.Key(df.format(j)));
          columnValue.setValue((df.format(i) + "_" + df.format(j) + "_Cell1DataV" + k).getBytes());
          columnValue.setTimestamp(timestamp++);
          columnCollectionCol1.addValue(rowKey, columnValue, 3);
        }
      }
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setValue((df.format(i) + "_Cell2Data").getBytes());
      columnCollectionCol2.addValue(rowKey, columnValue, 3);
    }
    memorySSTable.columnCollections.put("Col1", columnCollectionCol1);
    memorySSTable.columnCollections.put("Col2", columnCollectionCol2);

    System.out.println(NUM_ROWS + " rows inserted in MemorySSTable");
    
    return memorySSTable;
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Override
  protected DiskSSTable makeDiskSSTable() throws IOException {
    DiskSSTable diskSSTable = new DiskSSTable();
    Tablet tablet = new Tablet(conf, null, null, tabletInfo);
    diskSSTable.init(null, conf, tablet, 3);
    
    TabletMapFile col1TabletMapFile = new TabletMapFile(conf,
        tabletInfo, 
        "Col1",
        "file1", 
        3);

    TabletMapFile col2TabletMapFile = new TabletMapFile(conf,
        tabletInfo, 
        "Col2",
        "file1", 
        3);
    
    MapFileWriter writer1 = col1TabletMapFile.getMapFileWriter();
    MapFileWriter writer2 = col2TabletMapFile.getMapFileWriter();
    
    for(int i = 0; i < NUM_ROWS; i++) {
      if(i % 3 == 0) {
        continue;
      }
      Row.Key rowKey = new Row.Key(df.format(i));

      int numCells = getNumCells(i);
      long timestamp = (i % 2 == 0 ? TIMESTAMP1: TIMESTAMP2);
      for(int j = 0; j < numCells; j++) {
        for(int k = 0; k < 2; k++) {
          ColumnValue columnValue = new ColumnValue();
          columnValue.setRowKey(rowKey);
          columnValue.setCellKey(new Cell.Key(df.format(j)));
          columnValue.setValue((df.format(i) + "_" + df.format(j) + "_Cell1DataV" + k).getBytes());
          columnValue.setTimestamp(timestamp++);
          writer1.write(columnValue);
        }
      }
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setValue((df.format(i) + "_Cell2Data").getBytes());
      writer2.write(columnValue);
    }
    
    writer1.close();
    writer2.close();
    
    diskSSTable.addTabletMapFile("Col1", col1TabletMapFile);
    diskSSTable.addTabletMapFile("Col2", col2TabletMapFile);
    
    System.out.println(NUM_ROWS + " rows inserted in TabletMapFile");
    return diskSSTable;
  }

  //disk insert, memory insert
  public void testMultiVersion1() throws IOException {
    System.out.println("===================================== testMultiVersion1 =====================================");
    Row.Key rowKey = new Row.Key(df.format(50) + "_test1");

    TabletMapFile col1TabletMapFile = new TabletMapFile(conf,
        tabletInfo, 
        "Col1",
        "file2", 
        3);

    MapFileWriter writer1 = col1TabletMapFile.getMapFileWriter();

    long timestamp = 1;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test01".getBytes());
      
      writer1.write(columnValue);
    }
    writer1.close();
    diskSSTable.addTabletMapFile("Col1", col1TabletMapFile);
    
    ColumnCollection columnCollection = memorySSTable.columnCollections.get("Col1");
    timestamp = 2;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test01".getBytes());
      
      columnCollection.addValue(rowKey, columnValue, 3);
    }
    
    //assert data
    RecordSearcher searcher = new RecordSearcher(memorySSTable, diskSSTable, 3);
    searcher.search(rowKey, "Col1");
    
    ColumnValue[] columnValues = searcher.search(rowKey, "Col1");
    
    assertNotNull(columnValues);
    assertTrue(columnValues.length > 0);
    
    Row row = RowColumnValues.makeRow(
        new RowColumnValues[]{new RowColumnValues("Col1", rowKey, columnValues)});
    
    assertEquals(rowKey, row.getKey());
    List<Cell> cells = row.getCellList("Col1");    
    
    assertEquals(10, cells.size());
    
    for(int i = 0; i < cells.size(); i++) {
      assertEquals(new Cell.Key(df.format(i)), cells.get(i).getKey());
    }
  }
  
  //disk insert, memory delete
  public void testMultiVersion2() throws IOException {
    System.out.println("===================================== testMultiVersion2 =====================================");
    Row.Key rowKey = new Row.Key(df.format(50) + "_test2");

    TabletMapFile col1TabletMapFile = new TabletMapFile(conf,
        tabletInfo, 
        "Col1",
        "file3", 
        3);

    MapFileWriter writer1 = col1TabletMapFile.getMapFileWriter();

    long timestamp = 1;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test_disk".getBytes());
      
      writer1.write(columnValue);
    }
    writer1.close();
    diskSSTable.addTabletMapFile("Col1", col1TabletMapFile);
    
    ColumnCollection columnCollection = memorySSTable.columnCollections.get("Col1");
    timestamp = 2;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test_memory".getBytes());
      columnValue.setDeleted(true);
      columnCollection.addValue(rowKey, columnValue, 3);
    }
    
    //assert data
    RecordSearcher searcher = new RecordSearcher(memorySSTable, diskSSTable, 3);
    searcher.search(rowKey, "Col1");
    
    ColumnValue[] columnValues = searcher.search(rowKey, "Col1");

    assertNull(columnValues);
  }
  
  
  //disk insert, disk delete, memory insert
  public void testMultiVersion3() throws IOException {
    System.out.println("===================================== testMultiVersion3 =====================================");
    Row.Key rowKey = new Row.Key(df.format(50) + "_test3");

    //disk insert
    TabletMapFile col1TabletMapFile = new TabletMapFile(conf,
        tabletInfo, 
        "Col1",
        "file4", 
        3);

    MapFileWriter writer1 = col1TabletMapFile.getMapFileWriter();

    long timestamp = 1;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test01".getBytes());
      
      writer1.write(columnValue);
    }
    writer1.close();
    diskSSTable.addTabletMapFile("Col1", col1TabletMapFile);

    //disk delete
    TabletMapFile col1TabletMapFile2 = new TabletMapFile(conf,
        tabletInfo, 
        "Col1",
        "file5", 
        3);

    MapFileWriter writer2 = col1TabletMapFile2.getMapFileWriter();
    timestamp = 2;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test01".getBytes());
      columnValue.setDeleted(true);
      writer2.write(columnValue);
    }
    writer2.close();
    diskSSTable.addTabletMapFile("Col1", col1TabletMapFile2);

    //memory insert
    ColumnCollection columnCollection = memorySSTable.columnCollections.get("Col1");
    timestamp = 3;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test01".getBytes());
      columnCollection.addValue(rowKey, columnValue, 3);
    }
    
    //assert data
    RecordSearcher searcher = new RecordSearcher(memorySSTable, diskSSTable, 3);
    searcher.search(rowKey, "Col1");
    
    ColumnValue[] columnValues = searcher.search(rowKey, "Col1");
    
    assertNotNull(columnValues);
    assertTrue(columnValues.length > 0);
    
    Row row = RowColumnValues.makeRow(
        new RowColumnValues[]{new RowColumnValues("Col1", rowKey, columnValues)});
    
    assertEquals(rowKey, row.getKey());
    List<Cell> cells = row.getCellList("Col1");    
    
    assertEquals(10, cells.size());
    
    for(int i = 0; i < cells.size(); i++) {
      assertEquals(new Cell.Key(df.format(i)), cells.get(i).getKey());
      assertEquals(3, cells.get(i).getValue().getTimestamp());
    }
  }
  
  //disk insert, memory delete(i % 2 ==0)
  public void testMultiVersion4() throws IOException {
    System.out.println("===================================== testMultiVersion4 =====================================");
    Row.Key rowKey = new Row.Key(df.format(50) + "_test3");

    //disk insert
    TabletMapFile col1TabletMapFile = new TabletMapFile(conf,
        tabletInfo, 
        "Col1",
        "file4", 
        3);

    MapFileWriter writer1 = col1TabletMapFile.getMapFileWriter();

    long timestamp = 1;
    for(int i = 0; i < 10; i++) {
      ColumnValue columnValue = new ColumnValue();
      columnValue.setRowKey(rowKey);
      columnValue.setCellKey(new Cell.Key(df.format(i)));
      columnValue.setTimestamp(timestamp);
      columnValue.setValue("test01".getBytes());
      
      writer1.write(columnValue);
    }
    writer1.close();
    diskSSTable.addTabletMapFile("Col1", col1TabletMapFile);

    //memory delete
    ColumnCollection columnCollection = memorySSTable.columnCollections.get("Col1");
    timestamp = 2;
    for(int i = 0; i < 10; i++) {
      if(i % 2 == 0) {
        ColumnValue columnValue = new ColumnValue();
        columnValue.setRowKey(rowKey);
        columnValue.setCellKey(new Cell.Key(df.format(i)));
        columnValue.setTimestamp(timestamp);
        columnValue.setDeleted(true);
        columnValue.setValue("test01".getBytes());
        columnCollection.addValue(rowKey, columnValue, 3);
      }
    }
    
    //assert data
    RecordSearcher searcher = new RecordSearcher(memorySSTable, diskSSTable, 3);
    searcher.search(rowKey, "Col1");
    
    ColumnValue[] columnValues = searcher.search(rowKey, "Col1");
    
    assertNotNull(columnValues);
    assertTrue(columnValues.length > 0);
    
    Row row = RowColumnValues.makeRow(
        new RowColumnValues[]{new RowColumnValues("Col1", rowKey, columnValues)});
    
    assertEquals(rowKey, row.getKey());
    List<Cell> cells = row.getCellList("Col1");    
    
    assertEquals(5, cells.size());
    
    for(int i = 0; i < cells.size(); i++) {
      assertEquals(new Cell.Key(df.format(i * 2 + 1)), cells.get(i).getKey());
      assertEquals(1, cells.get(i).getValue().getTimestamp());
    }
    
    //cell key range
    RowFilter rowFilter = new RowFilter(rowKey);
    CellFilter cellFilter = new CellFilter("Col1");
    cellFilter.setCellKeys(new Cell.Key(df.format(3)), new Cell.Key(df.format(8)));
    rowFilter.addCellFilter(cellFilter);
    
    RowColumnValues[] values = searcher.search(rowFilter);
    assertNotNull(values);
    
    row = RowColumnValues.makeRow(values);
    assertEquals(rowKey, row.getKey());
    cells = row.getCellList("Col1");    
    
    assertEquals(3, cells.size());
    
    for(int i = 0; i < cells.size(); i++) {
      assertEquals(i + "th cellKey", new Cell.Key(df.format(3 + i * 2)), cells.get(i).getKey());
      assertEquals(1, cells.get(i).getValue().getTimestamp());
    }
  }
}
