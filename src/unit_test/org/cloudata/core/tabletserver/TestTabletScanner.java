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

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.ColumnCollection;
import org.cloudata.core.tabletserver.DiskSSTable;
import org.cloudata.core.tabletserver.MemorySSTable;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletScanner;

import junit.framework.TestCase;


/**
 * @author jindolk
 *
 */
public class TestTabletScanner extends TestCase {
  protected DecimalFormat df = new DecimalFormat("0000000000");
  protected int NUM_ROWS = 100;
  protected int NUM_CELL = 20;

  protected int TIMESTAMP1 = 10;
  protected int TIMESTAMP2 = 100;
  
  protected TabletInfo tabletInfo = new TabletInfo("T_TEST", "T_TEST_1234", Row.Key.MIN_KEY, Row.Key.MAX_KEY);
  CloudataConf conf = new CloudataConf();
  
  public void setUp() {
    conf.set("cloudata.filesystem", "local");
  }
  
  public void testScan() throws IOException {
    TabletScanner scanner = getTabletScanner(Row.Key.MIN_KEY, Row.Key.MAX_KEY, new CellFilter("Col1"));
    
    int count = 0;
    int rowNum = 0;
    ColumnValue[] columnValues = null;
    while( (columnValues = scanner.next()) != null ) {
      for(ColumnValue eachColumnValue: columnValues) {
        assertEquals(df.format(rowNum), eachColumnValue.getRowKey().toString());
        count ++;
        if(count % NUM_CELL == 0) {
          rowNum++;
        }
      }
    }
    
    assertEquals(NUM_ROWS * NUM_CELL, count);
    
    scanner = getTabletScanner(new Row.Key(df.format(10)), new Row.Key(df.format(49)), new CellFilter("Col1"));
    count = 0;
    rowNum = 10;
    columnValues = null;
    while( (columnValues = scanner.next()) != null ) {
      for(ColumnValue eachColumnValue: columnValues) {
        assertEquals(df.format(rowNum), eachColumnValue.getRowKey().toString());
        count ++;
        if(count % NUM_CELL == 0) {
          rowNum++;
        }
      }
    }
    assertEquals(40 * NUM_CELL, count);
  }
  
  private TabletScanner getTabletScanner(Row.Key startKey, Row.Key endKey, CellFilter cellFilter) throws IOException {
    return new TabletScanner(
        "id",
        conf,
        tabletInfo,
        startKey,
        endKey,
        "Col1",
        makeMemorySSTable(),
        new DiskSSTable(),
        cellFilter,
        1,
        true);
  }
  
  protected MemorySSTable makeMemorySSTable() throws IOException {
    MemorySSTable memorySSTable = new MemorySSTable();
    Tablet tablet = new Tablet(conf, null, null, tabletInfo);
    memorySSTable.init(null, conf, tablet, 3);
    
    ColumnCollection columnCollectionCol1 = new ColumnCollection(conf);
    ColumnCollection columnCollectionCol2 = new ColumnCollection(conf);
    
    for(int i = 0; i < NUM_ROWS; i++) {
      Row.Key rowKey = new Row.Key(df.format(i));
      long timestamp = (i % 2 == 0 ? TIMESTAMP1: TIMESTAMP2);
      for(int j = 0; j < NUM_CELL; j++) {
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

    System.out.println(NUM_ROWS + " rows inserted");
    return memorySSTable;
  }
}
