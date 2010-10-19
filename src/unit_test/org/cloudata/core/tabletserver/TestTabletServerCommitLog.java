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

import java.text.DecimalFormat;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.commitlog.CommitLogServer;
import org.cloudata.core.common.CloudataClusterTestCase;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * 1. put data
 * 2. CommitLogServer fail, TabletServer fail
 * 3. read data
 * @author jindolk
 *
 */
public class TestTabletServerCommitLog extends CloudataClusterTestCase {
  public TestTabletServerCommitLog(String name) {
    super(name);
  }

  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestTabletServerCommitLog("testCommitLogServerFail"));
    return suite;
  }
  
  public void testCommitLogServerFail() throws Exception {
    final DecimalFormat df = new DecimalFormat("0000000000");
    String tableName = "T_TEST";
    //create table
    TableSchema tableSchema = new TableSchema(tableName);
    tableSchema.addColumn("COL1");
    tableSchema.addColumn("COL2");
    CTable.createTable(conf, tableSchema);
    
    //put data
    final CTable ctable = CTable.openTable(conf, tableName);
    
    for(int i = 0; i < 50; i++) {
      Row row = new Row(new Row.Key(df.format(i)));
      row.addCell("COL1", new Cell(Cell.Key.EMPTY_KEY, ("test data" + i).getBytes()));
      row.addCell("COL2", new Cell(new Cell.Key("CK" + df.format(i)), null));
      ctable.put(row);
    }
    
    //kill commit log server
    CommitLogServer[] commitLogServers = cluster.getCommitLogServers();
    System.out.println("================================ commit log server killed ================================");
    commitLogServers[1].stop();

    Thread putThread = new Thread() {
      public void run() {
        try {
          //put next data
          for(int i = 100; i < 200; i++) {
            Row row = new Row(new Row.Key(df.format(i)));
            row.addCell("COL1", new Cell(Cell.Key.EMPTY_KEY, ("test data" + i).getBytes()));
            row.addCell("COL2", new Cell(new Cell.Key("CK" + df.format(i)), null));
            ctable.put(row);
            Thread.sleep(20);
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    };
    
    putThread.start();

    Thread.sleep(5000);
    //put next data
    for(int i = 50; i < 100; i++) {
      Row row = new Row(new Row.Key(df.format(i)));
      row.addCell("COL1", new Cell(Cell.Key.EMPTY_KEY, ("test data" + i).getBytes()));
      row.addCell("COL2", new Cell(new Cell.Key("CK" + df.format(i)), null));
      ctable.put(row);
    }
    
    long startTime = System.currentTimeMillis();
    while(true) {
      if(!putThread.isAlive()) {
        break;
      }
      
      if(System.currentTimeMillis() - startTime > 60 * 1000) {
        fail("put thread fail");
        return;
      }
      Thread.sleep(5 * 1000);
    }

    //kill tablet server
    List<TabletInfo> tablets = listTabletInfos(tableName);
    String hostName = tablets.get(0).getAssignedHostName();
    cluster.stopTabletServer(hostName);
    Thread.sleep(5000);
    
    //check data
    startTime = System.currentTimeMillis();
    while(true) {
      tablets = listTabletInfos(tableName);
      if(tablets != null && tablets.size() > 0) {
        String reAssignedHostName = tablets.get(0).getAssignedHostName();
        if(reAssignedHostName != null && !reAssignedHostName.equals(hostName)) {
          break;
        }
      }
      
      if(System.currentTimeMillis() - startTime > 20 * 1000) {
        fail("re-assignmenet fail");
        return;
      }
    }
    
    for(int i = 0; i < 200; i++) {
      Row row = ctable.get(new Row.Key(df.format(i)));
      assertNotNull(row);
      
      Cell cell1 = row.getFirst("COL1");
      assertNotNull(cell1);
      assertEquals("test data" + i, cell1.getValueAsString());
      
      Cell cell2 = row.getFirst("COL2");
      assertNotNull(cell2);
      assertEquals("CK" + df.format(i), cell2.getKey().toString());
    }
  }
}
