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

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tabletserver.DiskSSTable;
import org.cloudata.core.tabletserver.MemorySSTable;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileWriter;


/**
 * @author jindolk
 *
 */
public class TestRecordSearcherDisk extends TestRecordSearcher {
  public TestRecordSearcherDisk(String name) throws IOException {
    super(name);
  }

  @Override
  protected MemorySSTable makeMemorySSTable() throws IOException {
    MemorySSTable memorySSTable = new MemorySSTable();
    Tablet tablet = new Tablet(conf, null, null, tabletInfo);
    memorySSTable.init(null, conf, tablet, 3);
    
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
}
