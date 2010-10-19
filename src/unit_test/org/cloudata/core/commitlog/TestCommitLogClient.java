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
package org.cloudata.core.commitlog;

import java.io.IOException;
import java.util.Random;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.commitlog.CommitLogClient;
import org.cloudata.core.commitlog.TransactionData;
import org.cloudata.core.commitlog.pipe.Bulk;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.MiniCommitLogCluster;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tabletserver.CommitLog;

import junit.framework.TestCase;


public class TestCommitLogClient extends TestCase {
  MiniCommitLogCluster cluster;
  CloudataConf conf = new CloudataConf();
  static byte[] largeBytes;
  
  static {
    largeBytes = new byte[Bulk.DEFAULT_BUFFER_SIZE];
    
    for(int i = 0; i < largeBytes.length; i++) {
      largeBytes[i] = (byte) (new Random().nextInt() % 0xff);
    }
  }

  protected void setUp() throws Exception {
    cluster = MiniCommitLogCluster.create(conf);
  }

  protected void tearDown() throws Exception {
    try {
      Thread.sleep(1000);
    } catch(Exception e) {
    }
    cluster.shutdown();
  }
  
  public void testPutLargeData() {
    CommitLog[] insertLogList = null;
    try {
      insertLogList = createLargeCommitLogList(Constants.LOG_OP_ADD_COLUMN_VALUE);
    } catch (IOException e1) {
      e1.printStackTrace();
      assertTrue(false);
    }
    
    CommitLogClient client = null;
    try {
      String tabletName = "TEST_TABLET_" + System.currentTimeMillis();
      client = new CommitLogClient(conf);
      client.removeAllLogs(tabletName);
      
      client.open();
      
      client.startWriting(1, tabletName);
      
      for(CommitLog log : insertLogList) {
        client.append(log);
      }
      
      int size = client.commitWriting();
      System.out.println("size : " + size);
      TransactionData[] log = client.readLastLogFile(tabletName);
      
      assertEquals(1, log.length);
      assertEquals(3, log[0].logList.length);
      assertEquals(size, client.getLogFileSize(tabletName));
      
      byte[] logData = log[0].logList[0].getValue();
      assertEquals(largeBytes.length, logData.length);
      
      for(int i = 0; i < largeBytes.length; i++) {
        if (largeBytes[i] != logData[i]) {
          System.out.println("index : " + i + " is different");
          assertTrue(false);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  public void testMultiThread() {
    final int NUM_THREAD = 20;
    String dirName = "TEST_" + System.currentTimeMillis();
    Thread[] threadList = new Thread[NUM_THREAD];

    for(int i = 0; i < NUM_THREAD; i++) {
      threadList[i] = new Thread(new TestRunnable(dirName));
      threadList[i].start();
    }

    for(int i = 0; i < NUM_THREAD; i++) {
      try {
        threadList[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        assertTrue(false);
      }
    }
    
    try {
      CommitLogClient client = new CommitLogClient(conf);
      long[] sizes = client.getAllLogFileSize(dirName);
      
      for(int i = 0; i < sizes.length - 1; i++) {
        assertEquals(sizes[i], sizes[i + 1]);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
  
  class TestRunnable implements Runnable {
    static final int NUM_TRY = 100;
    String dirName;
    TestRunnable(String dirName) {
      this.dirName = dirName;
    }
    
    public void run() {
      CommitLog[] logList = null;
      
      try {
        logList = createCommitLogList(Constants.LOG_OP_ADD_COLUMN_VALUE);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      } 
      
      try {
        CommitLogClient client = new CommitLogClient(conf);
        client.open();
        
        for(int i = 0; i < NUM_TRY; i++) {
          client.startWriting(i, dirName);
          
          for(int j = 0; j < logList.length; j++) {
            client.append(logList[j]);
          }
          
          client.commitWriting();
        }
        
        client.close();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }

  private CommitLog[] createCommitLogList(int operation) throws IOException {
    CommitLog[] logList = new CommitLog[3];
    for(int i = 0; i < logList.length; i++) {
      long timestamp = System.currentTimeMillis();
      logList[i] = new CommitLog(operation, 
          "TEST_TABLET", 
          new Row.Key("TEST_ROWKEY_" + timestamp),
          "TEST_COLUMN_NAME",
          new Cell.Key("TEST_COLUMN_KEY_" + timestamp),
          timestamp,
          String.valueOf(timestamp).getBytes());  
    }
    return logList;
  }
  
  private CommitLog[] createLargeCommitLogList(int operation) throws IOException {
    CommitLog[] logList = new CommitLog[3];
    for(int i = 0; i < logList.length; i++) {
      long timestamp = System.currentTimeMillis();
      logList[i] = new CommitLog(operation, 
          "TEST_TABLET", 
          new Row.Key("TEST_ROWKEY_" + timestamp),
          "TEST_COLUMN_NAME",
          new Cell.Key("TEST_COLUMN_KEY_" + timestamp),
          timestamp,
          largeBytes);  
    }
    return logList;
  }
}
