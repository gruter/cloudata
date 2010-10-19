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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.commitlog.CommitLogStatus;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.MiniCommitLogCluster;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CommitLogFileSystem;
import org.cloudata.core.fs.CommitLogFileSystemIF;
import org.cloudata.core.tabletserver.CommitLog;


public class TestCommitLogFileSystem extends TestCase {
  static final Log LOG = LogFactory.getLog(TestCommitLogFileSystem.class);
  MiniCommitLogCluster cluster;
  CommitLogFileSystemIF clfs;
  ZooKeeper zk;
  final String TABLET_NAME = "UNIT_TEST_TABLET";
  
  protected void setUp() throws Exception {
    CloudataConf conf = new CloudataConf();
    conf.set("cloudata.commitlog.filesystem", "pipe");
    conf.set("cloudata.filesystem", "local");
    conf.set("cloudata.local.temp", System.getProperty("user.home") + File.separator + ".cloudata_local");
    cluster = MiniCommitLogCluster.create(conf);
    // Pipe기반 commitLogFileSystem은 생성시 TabletServerIF와 LockService를 인자로 받을 필요가 없다.
    clfs = CommitLogFileSystem.getCommitLogFileSystem(conf, null, null);
  }

  protected void tearDown() throws Exception {
    sleep(1);
    clfs.close(TABLET_NAME, true);
    //clfs.format();
    clfs = null;
    sleep(1);
    cluster.shutdown();
    cluster = null;
  }
  
  private void sleep(int sec) {
    try { Thread.sleep(sec * 1000); } catch(Exception e) { }
  }
  
  public void testBasicInsertAndGet() {
    try {
      if (clfs.exists(TABLET_NAME)) {
        clfs.delete(TABLET_NAME);
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    CommitLog[] logList = new CommitLog[10];
    long globalTimestamp = System.currentTimeMillis();
    String txId = "TX_ID_" + globalTimestamp; 
    
    for(int i = 0; i < logList.length; i++) {
      try {
        logList[i] = createCommitLog(globalTimestamp, i);
      } catch (IOException e) {
        e.printStackTrace();
        assertTrue(false);
      }
    }

    appendCommitLog(TABLET_NAME, logList, txId);

    checkTabletExists(true);

    tryToVerifyLogs(txId);
    tryToReadLogs(logList);
  }
  
  public void testMultiThreadWriteToSingleTablet() {
    final int NUM_TRY = 50;
    final int NUM_THREAD = 20;

    for (int i = 0; i < NUM_TRY; i++) {
      final CountDownLatch latch = new CountDownLatch(NUM_THREAD);

      Runnable run = new Runnable() {
        public void run() {
          CommitLog[] logList = new CommitLog[NUM_THREAD];
          long globalTimestamp = System.currentTimeMillis();
          String txId = "TX_ID_" + globalTimestamp;

          for (int i = 0; i < logList.length; i++) {
            try {
              logList[i] = createCommitLog(globalTimestamp, i);
            } catch (IOException e) {
              e.printStackTrace();
              assertTrue(false);
            }
          }

          appendCommitLog(TABLET_NAME, logList, txId);

          latch.countDown();
        }
      };

      for(int j = 0; j < NUM_THREAD; j++) {
        new Thread(run).start();
      }

      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        assertTrue(false);
      }
      
//      try {
//        Thread.sleep(1000);
//      } catch(Exception e) {
//      }
    }
  }

  private CommitLog createCommitLog(long globalTimestamp, int i) throws IOException {
    byte[] value = (i + "_abcdefghijklmnopqrstuvwxyz0123456789").getBytes(); 
    return new CommitLog(Constants.LOG_OP_ADD_COLUMN_VALUE, 
        TABLET_NAME, 
        new Row.Key("Row.Key_" + i), 
        new String("ColumnName_" + i), 
        new Cell.Key("Cell.Key_" + i), 
        globalTimestamp + i, 
        value);
  }

  private void tryToReadLogs(CommitLog[] logList) {
    try {
      clfs.open(TABLET_NAME, false);
      CommitLog[] readLogList = new CommitLog[logList.length];
      
    } catch(IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  private void tryToVerifyLogs(String txId) {
    try {
      CommitLogStatus status = clfs.verifyCommitlog(TABLET_NAME);
      //assertEquals(1, status.getFileCount()); // NOT IMPLEMENTED
      //assertEquals(9, status.getMaxSeq()); // NOT IMPLEMENTED
      assertFalse(status.isNeedCompaction());
//      assertEquals(txId, status.getMaxTxId()); // NOT IMPLEMENTED
    } catch(IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  private void appendCommitLog(String tabletName, CommitLog[] logList, String txId) {
    try {
      clfs.open(tabletName, true);
   
      for(int i = 0; i < logList.length; i++) {
        clfs.addCommitLog(tabletName, txId, i, logList[i]);
      }
      
      clfs.finishAdding(tabletName, txId);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  private void checkTabletExists(boolean shouldExists) {
    try {
      assertTrue(clfs.exists(TABLET_NAME) == shouldExists);
    } catch (IOException e1) {
      e1.printStackTrace();
      assertTrue(false);
    }
  }
}
