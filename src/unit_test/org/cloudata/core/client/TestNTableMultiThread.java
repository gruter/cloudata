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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;

import junit.framework.TestCase;


/**
 * @author jindolk
 *
 */
public class TestNTableMultiThread extends TestCase {
  private String tableName = "T_TEST_NTABLE_MULTITHREAD";
  private int ROW_COUNT = 1024 * 1024;
  private int THREAD_COUNT = 10;
  
  private CloudataConf conf;
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestNTableMultiThread.class);
  }
  
  public void setUp() throws Exception {
    conf = new CloudataConf();
    if(CTable.existsTable(conf, tableName)) {
      CTable.dropTable(conf, tableName);
    }
    TableSchema tableSchema = new TableSchema(tableName);
    tableSchema.addColumn("Column1");
    
    CTable.createTable(conf, tableSchema);
  }
  
  public void tearDown() throws Exception {
    //NTable.dropTable(conf, tableName);
  }
  
  public void testPut() throws Exception {
    List<PutThread> threads = new ArrayList<PutThread>();
    
    for(int i = 0; i < THREAD_COUNT; i++) {
      PutThread t = new PutThread(String.valueOf(i + 1));
      t.start();
      threads.add(t);
    }

    for(PutThread eachThread: threads) {
      try {
        eachThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    CTable ctable = CTable.openTable(conf, tableName);
    TableScanner scanner = ScannerFactory.openScanner(ctable, "Column1");

    int count = 0;
    try {
      Row row = null;
      while( (row = scanner.nextRow()) != null ) {
        count++;
      }
    } finally {
      scanner.close();
    }
    
    assertEquals(ROW_COUNT * THREAD_COUNT, count);
  }
  
  class PutThread extends Thread {
    String rkPrefix;
    
    long openTime = 0;
    
    public PutThread(String rkPrefix) {
      this.rkPrefix = rkPrefix;
    }
    
    public void run() {
      put();
    }
    
    private void put() {
      DecimalFormat df = new DecimalFormat("0000000000");
      Random rand = new Random();
      
      byte[] data = new byte[1000];
      
      try {
        for(int i = 0; i < ROW_COUNT; i++) {
          long start = System.nanoTime();
          CTable ctable = CTable.openTable(conf, tableName);
          openTime += (System.nanoTime() - start);
          
          Row.Key rowKey = new Row.Key(rkPrefix + df.format(i));
          
          rand.nextBytes(data);
          
          Row row = new Row(rowKey);
          row.addCell("Column1", new Cell(Cell.Key.EMPTY_KEY, data));
          ctable.put(row);
          
          if((i + 1) % 10000 == 0) {
            System.out.println("Thread " + rkPrefix + ": " + i + " inserted, openTime: " + openTime + " ns");
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        TestCase.fail(e.getMessage());
      }
      System.out.println("Thread " + rkPrefix + ": end of insert, openTime: " + openTime + " ns");
    }
  }
}
