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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.commitlog.CommitLogInterruptedException;
import org.cloudata.core.commitlog.CommitLogOverLoadedException;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.IdGenerator;
import org.cloudata.core.common.Latch;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.CommitLogException;
import org.cloudata.core.common.exception.TxException;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.NumberUtil;
import org.cloudata.core.common.util.SizeOf;
import org.cloudata.core.fs.CommitLogFileSystemIF;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;
import org.cloudata.core.tabletserver.action.ActionChecker;
import org.cloudata.core.tabletserver.action.ActionFinalizer;
import org.cloudata.core.tabletserver.action.ApplyAction;
import org.cloudata.core.tabletserver.action.FlushAction;
import org.cloudata.core.tabletserver.action.MajorCompactionAction;
import org.cloudata.core.tabletserver.action.MinorCompactionAction;
import org.cloudata.core.tabletserver.action.ScannerAction;
import org.cloudata.core.tabletserver.action.TabletLockAction;
import org.cloudata.core.tabletserver.action.TabletSplitAction;
import org.cloudata.core.tabletserver.action.TxAction;


/**
 * commit: CommitLogMerge를 하면 안됨 scanner opened: Major Compaction, Minor
 * Compaction, Split or Merge 못함. drop, stop은 기다렸다가 close되면 시작 MinorCompaction:
 * MajorCompaction, Split, Merge, ScanOpen 못하고, drop, stop은 기다렸다가 종료되면 시작
 * MajorCompaction: MinorCompaction, Split, Merge, ScanOpen 못하고, drop, stop은
 * 기다렸다가 종료되면 시작 Split or Merge: Major Compaction, Minor Compaction, drop, stop,
 * ScanOpen 못함. commit은 Split/Merge 작업중 MemTablet에 관련된 작업 진행중에만 못함 drop or stop:
 * all stop
 * 
 * @author babokim
 * 
 */
public class Tablet implements Constants {
  public static final String DIR_TEMP_SPLIT = "/temp/split/";
  public static final String DIR_TEMP_MINOR = "/temp/minor_compaction/";
  public static final String DIR_TEMP_MAJOR = "/temp/major_compaction/";
  public static final String DIR_TEMP_UPLOAD = "/temp/upload/";

  public static final int MAX_COMMITLOG_ERROR = 10;
  
  // FIXME DiskSSTable에 대한 Cache 기능: 사용자의 정의에 따라 특정 Table의 경우 DiskSSTable의 모든
  // 데이터를 메모리에 올려 서비스 한다.
  public static final Log LOG = LogFactory.getLog(Tablet.class.getName());
  // public static Random rand = new Random();

  protected final TabletInfo tabletInfo;

  protected final MemorySSTable memorySSTable;

  protected AtomicReference<DiskSSTable> diskSSTable = new AtomicReference<DiskSSTable>();

  protected final ConcurrentHashMap<TxId, TxValue> txMap = new ConcurrentHashMap<TxId, TxValue>();

  // lockedRow에 대해 주기적으로 돌면서 아무런 반응이 없는 상태에서 lock을 오랫동안 잡고 있는 경우 해제 ->
  // TabletServer의 TxListener에서 처리
  // rowkey, lock time
  protected final ConcurrentHashMap<Row.Key, Long> lockedRow = new ConcurrentHashMap<Row.Key, Long>();

  protected CloudataConf conf;

  private CloudataFileSystem fs;

  // scannerId, TabletScanner
  private final Map<String, TabletScanner> tabletScanners = Collections
      .synchronizedMap(new HashMap<String, TabletScanner>());

  private AtomicBoolean ready = new AtomicBoolean(false);

  protected final ActionChecker actionChecker;

  protected final TableSchema table;

  protected long maxTabletSizeByte;

  protected MinorCompactionAction minorCompactionAction;

  protected MajorCompactionAction majorCompactionAction;

  protected TabletSplitAction tabletSplitAction;

  protected boolean testMode = false;

  protected TabletServer tabletServer;

  private boolean droped;

  private AtomicInteger inCommitLogError = new AtomicInteger(0);

  private int flushConcurrency;

  private long maxMemorySSTableSize;

  private final double splitRatioConstants;

  private AtomicBoolean forceMinorCompactionLock = new AtomicBoolean(false);
  
  private boolean spliting = false;

  private Tablet[] splitingTablets;
  
  private AtomicBoolean applyLock = new AtomicBoolean(false);

  //protected AtomicBoolean minorCpmpactionLock = new AtomicBoolean(false);

  protected Latch latch;
  
  public Tablet(CloudataConf conf, TableSchema table, TabletServer tabletServer,
      TabletInfo tabletInfo) throws IOException {

    this.tabletServer = tabletServer;
    this.conf = conf;
    this.testMode = conf.getBoolean("testmode", false);
    this.fs = CloudataFileSystem.get(conf);
    splitRatioConstants = tabletServer.getTabletCount() < 1 ? 0.8 : NumberUtil.randomWithRange(0.2);

    if (table == null) {
      throw new IOException("No table info:" + tabletInfo);
    }
    if (tabletServer != null) {
      this.actionChecker = new ActionChecker(conf, tabletServer.compactionExecutor,
          tabletServer.splitExecutor, tabletServer.actionExecutor, tabletServer.threadGroup,
          tabletInfo);
    } else {
      this.actionChecker = new ActionChecker(conf, null, null, null, null, tabletInfo);
    }

    this.table = table;
    this.tabletInfo = tabletInfo;
    
    this.latch = new Latch(tabletInfo.getTabletName());
    
    this.memorySSTable = new MemorySSTable();
    this.memorySSTable.init(tabletServer, conf, this, table.getNumOfVersion());

    this.diskSSTable.set(new DiskSSTable());
    this.diskSSTable.get().init(tabletServer, conf, this, table.getNumOfVersion());

    this.minorCompactionAction = new MinorCompactionAction();
    this.minorCompactionAction.init(conf, this);
    this.majorCompactionAction = new MajorCompactionAction(this);
    this.tabletSplitAction = new TabletSplitAction(this, tabletServer);

    this.maxTabletSizeByte = (long) (conf.getFloat("maxTabletSize", 200.0f) * 1024.0f * 1024.0f);
    this.flushConcurrency = conf.getInt("tabletServer.flushConcurrency", 10);
    this.maxMemorySSTableSize = conf.getLong("memory.maxTabletSize", 400) * 1024L * 1024L;
  }

  public void setSplitingTablets(Tablet[] tablets) {
    this.splitingTablets = tablets;
  }
  
  public TableSchema getTable() {
    return table;
  }

  public CloudataConf getConf() {
    return conf;
  }

  public void setDiskSSTable(DiskSSTable diskSSTable) {
    this.diskSSTable.set(diskSSTable);
  }

  public void print() throws IOException {
    System.out.println(tabletInfo.toString());
    System.out.println("------------------ Memory Data -----------------");
    memorySSTable.print();
    // System.out.println("------------------ Disk Data -----------------");
    // diskSSTable.print();
  }

//  public void setMinorCpmpactionLock(boolean lock) {
//    minorCpmpactionLock.set(lock);
//  }
  /**
   * Tablet이 TabletServer에 할당되면 MapFile의 index 파일을 읽어 index를 구성하고 CommitLog 파일을
   * 읽어 MemoryTablet을 구성한다.
   * 
   * @throws IOException
   */
  public boolean load() throws IOException {
    // long startTime = System.currentTimeMillis();

    // load commit log file and add MemoryTablet
    boolean memoryLoadResult = false;

    int retry = 1;
    while (true) {
      try {
        memoryLoadResult = memorySSTable.loadFromCommitLog();
        break;
      } catch (CommitLogOverLoadedException e) {
        retry++;
        if (retry >= 20) {
          throw e;
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException err) {
          return false;
        }
        LOG.info("occur CommitLogOverLoadedException while load(), but retry:" + retry);
      }
    }

    // load disk tablet index
    diskSSTable.get().load();

    return memoryLoadResult;
  }

  /**
   * 데이터를 조회한다.
   * 
   * @param rowKey
   * @param columnName
   * @param columnKey
   * @return
   * @throws IOException
   */
  public ColumnValue get(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    return new RecordSearcher(this).search(rowKey, columnName, cellKey);
  }

  public RowColumnValues[] get(RowFilter filter) throws IOException {
    return new RecordSearcher(this).search(filter);
  }

  /**
   * 데이터를 조회한다. !!!주의사항!!!! return되는 ColumnValue 값의 rowKey에는 값이 없다. 이것은 실시간 처리의
   * 경우 데이터 전송량이 많아지는 것을 방지하기 위해서이다.
   * 
   * @param rowKey
   * @param columnName
   * @return
   * @throws IOException
   */
  public ColumnValue[] get(Row.Key rowKey, String columnName) throws IOException {
    return new RecordSearcher(this).search(rowKey, columnName);
  }

  /**
   * 특정 Row의 지정된 컬럼들의 데이터를 조회한다. row에 저장된 컬럼의 데이터가 정해진 사이즈보다 많은 경우 Exception이
   * 발생한다.
   * 
   * @param rowKey
   * @param columnNames
   * @return
   * @throws IOException
   */
  public ColumnValue[][] get(Row.Key rowKey, String[] columnNames) throws IOException {
    return new RecordSearcher(this).search(rowKey, columnNames);
  }

  /**
   * 특정 Row의 모든 컬럼의 데이터를 조회한다.
   * 
   * @param rowKey
   * @return
   * @throws IOException
   */
  public ColumnValue[][] get(Row.Key rowKey) throws IOException {
    // FIXME StopAction이 실행중인 경우에는 Exception을 보낸다.
    // (StopAction중인 경우에는 다른 서버로 Tablet이 이관중이고 이관받은 서버가 MetaData를 수정했음에도 불구하고
    // client에 있는 cache에 의해 계속 이전서버로 데이터를 요청할 수 있기 때문이다.
    // Client에서는 get 수행시 Exception이 발생하면 retry를 한다.

    // FIXME Column 순서에 대한 정의(사용자가 생성한 순서 or 알파벳순)
    int columnCount = table.getColumns().size();
    String[] columnNames = table.getColumns().toArray(new String[columnCount]);

    return get(rowKey, columnNames);
  }

  /**
   * 입력받은 rowKey와 가장 가까운 Row.Key중 가장 큰 Row.Key를 가지는 row의 값을 조회한다. 주로 Root, Meta
   * 테이블의 데이터를 이용하여 TabletInfo를 조회할 때 사용된다.
   * 
   * @param rowKey
   * @return
   * @throws IOException
   */
  public ColumnValue getClosestMeta(Row.Key rowKey) throws IOException {
    // SortedSet<ColumnValue> closestRow.Keys = new TreeSet<ColumnValue>();

    // 메모리로부터 삭제되지 않은 rowKey보다 크거나 같은 값 중 가장 작은 값을 가져 온다.
    ColumnValue closestValueFromMemory = memorySSTable.findClosest(rowKey,
        META_COLUMN_NAME_TABLETINFO);

    ColumnValue closestValueFromDisk = diskSSTable.get().findClosestMeta(rowKey,
        META_COLUMN_NAME_TABLETINFO, true);

    while (true) {
      // Disk에서 가져온 값이 삭제된 값인 경우 다시 가져온다.
      if (closestValueFromDisk != null
          && memorySSTable.isDeleted(closestValueFromDisk.getRowKey(), META_COLUMN_NAME_TABLETINFO)) {
        closestValueFromDisk = diskSSTable.get().findClosestMeta(closestValueFromDisk.getRowKey(),
            META_COLUMN_NAME_TABLETINFO, false);
      } else {
        break;
      }
    }

    if (closestValueFromMemory == null && closestValueFromDisk == null) {
      LOG.warn("closestRowKey is null:" + rowKey);
      //memorySSTable.print();
      return null;
    } else if (closestValueFromMemory == null && closestValueFromDisk != null) {
      return closestValueFromDisk;
    } else if (closestValueFromMemory != null && closestValueFromDisk == null) {
      return closestValueFromMemory;
    } else {
      if (closestValueFromMemory.getRowKey().compareTo(closestValueFromDisk.getRowKey()) <= 0) {
        return closestValueFromMemory;
      } else {
        return closestValueFromDisk;
      }
    }
  }

  /**
   * MemTablet의 현재 메모리 사이즈를 반환한다.
   * 
   * @return
   */
  public int getMemtableSize() {
    return memorySSTable.getTabletSize();
  }

  public void removeRow(TxId txId, long timestamp) throws IOException {
    // FIXME Thread로 처리할 것인지 그냥 처리할 것인지..
    // 그냥 처리할 경우 Row의 column 값이 많은 경우 client에서 timeout exception이 발생할 수 있음.

    TxValue txValue = getTxValue(txId);
    Row.Key rowKey = txValue.getRowKey();

    if (timestamp == 0) {
      timestamp = System.currentTimeMillis();
    }

    // boolean isdeleted = false;
    for (String columnName : table.getColumns()) {
      // FIXME 값을 제대로 조회하지 못하는 경우가 발생. 특히 META 삭제 처리를 할 경우
      ColumnValue[] columnValues = get(rowKey, columnName);

      // LOG.info("start RemoveRow:" + tabletInfo.getTabletName() + "," +
      // columnName + "," + columnValues);
      if (columnValues == null || columnValues.length == 0) {
        continue;
      }

      for (ColumnValue columnValue : columnValues) {
        // get() 메소드의 경우 columnValue의 rowKey에는 값이 없다.
        // LOG.info("removeRow:" + columnValue);
        columnValue.setRowKey(rowKey);
        deleteColumnValue(txId, columnName, columnValue.getCellKey(), timestamp);
        // isdeleted = true;
      }
    }
  }

  /**
   * 특정 row에 컬럼 데이터를 저장한다.
   * 
   * @param txId
   * @param columnName
   * @param columnValues
   */
  public void insertColumnValue(final TxId txId, final String columnName,
      final ColumnValue[] columnValues) throws IOException {
    TxValue txValue = getTxValue(txId);
    if (!table.getColumns().contains(columnName)) {
      throw new TxException("Invalid column [" + columnName + "]");
    }

    for (ColumnValue record : columnValues) {
      long timestamp = txId.isSystemTimestamp() ? System.currentTimeMillis() : record
          .getTimestamp();
      if (timestamp < 0) {
        continue;
      }

      CommitLog commitLog = new CommitLog(Constants.LOG_OP_ADD_COLUMN_VALUE, tabletInfo
          .getTableName(), txValue.getRowKey(), columnName, record.getCellKey(), timestamp, record
          .getValue());
      txValue.addCommitLog(commitLog);
    }
  }

  /**
   * 특정 Row의 컬럼 값을 삭제한다.
   * 
   * @param txId
   * @param columnName
   * @param columnKey
   * @param timestamp
   */
  public boolean deleteColumnValue(TxId txId, String columnName, Cell.Key columnKey, long timestamp)
      throws IOException {
    TxValue txValue = getTxValue(txId);
    if (!table.getColumns().contains(columnName)) {
      throw new TxException("Invalid column [" + columnName + "]");
    }
    CommitLog commitLog = new CommitLog(Constants.LOG_OP_DELETE_COLUMN_VALUE, tabletInfo
        .getTableName(), txValue.getRowKey(), columnName, columnKey, timestamp, null);
    txValue.addCommitLog(commitLog);

    return true;
  }

  private TxValue getTxValue(TxId txId) throws IOException {
    TxValue txValue = txMap.get(txId);

    if (txValue == null) {
      throw new TxException("Tx [" + txId + "] not started");
    }
    return txValue;
  }

  /**
   * 트렌젝션을 취소시킨다. 트렌젝션 시작 후 입력된 값은 반영되지 않는다.
   * 
   * @param txId
   */
  public Row.Key rollback(TxId txId) throws IOException {
    TxValue txValue = getTxValue(txId);
    Row.Key rowKey = txValue.getRowKey();
    txMap.remove(txId);

    actionChecker.endAction(new TxAction(txId));

    return rowKey;
  }

  public boolean startFlush(TxId txId) throws IOException {
    if (!ready.get() || droped) {
      return false;
    }
    if (SizeOf.getRealFreeMemory() < 100 || getMemtableSize() > maxMemorySSTableSize) {
      return false;
    }

    FlushAction flushAction = new FlushAction(txId, flushConcurrency);

    if (!actionChecker.startAction(flushAction)) {
      return false;
    }
    return true;
  }

  public void setApplyLock(boolean lock) {
    applyLock.getAndSet(lock);  
  }
  
  public boolean getApplyLock() {
    return applyLock.get();
  }
  
  public void endFlush(TxId txId) throws IOException {
    FlushAction flushAction = new FlushAction(txId, flushConcurrency);
    actionChecker.endAction(flushAction);
  }

  public boolean blockLatch() {
    try {
      latch.block();
      return true;
    } catch (InterruptedException e) {
      return false;
    }
  }
  
  public void unblockLatch() {
    latch.unblock();
  }
  
  public boolean apply(Row.Key rowKey, TxId txId, CommitLog[] commitLogList, boolean saveLog)
      throws IOException {
    if(applyLock.get() || !ready.get()) {
      return false;
    }
    try {
      if (!latch.enter(100, txId.getTxId())) {
        //LOG.debug(tabletInfo.getTabletName() + " denied to enter in Tablet.latch");
        return false;
      }
    } catch (InterruptedException e) {
      LOG.debug("Latch.enter Interrupted: " + tabletInfo.getTabletName());
      return false;
    } 
    
    ApplyAction applyAction = null;
    try {
      for (CommitLog commitLog : commitLogList) {
        if (!tabletInfo.belongRowRange(commitLog.getRowKey())){
          LOG.warn(tabletInfo.getTabletName() + ":" + commitLog.getRowKey() + " is out of range:" + tabletInfo);
          throw new IOException(commitLog.getRowKey() + " is out of range:" + tabletInfo);
        }
      }
      applyAction = new ApplyAction(txId);
  
      if (!actionChecker.startAction(applyAction)) {
        return false; // client will tries again
      }
  
      if (!ready.get() || droped) {
        throw new IOException("[" + tabletInfo + "] tablet is not ready:ready=" + ready + ", droped=" + droped);
      }
      
      if(splitingTablets != null) {
        Tablet matchedTablet = splitingTablets[0];
        if(rowKey.compareTo(matchedTablet.getTabletInfo().getEndRowKey()) > 0) {
          matchedTablet = splitingTablets[1];
        }
        
        if(!matchedTablet.apply(rowKey, txId, commitLogList, saveLog)) {
          LOG.info(tabletInfo.getTabletName() + " can't apply to spliting tablet " + matchedTablet.getTabletInfo() + "[" + rowKey + "]");
          return false;
        }
      }
      
      txMap.put(txId, new TxValue(rowKey));

      return  mutateMemorySSTable(txId, rowKey, commitLogList, saveLog);
    } finally {
      try {
        txMap.remove(txId);
        if(applyAction != null) {
          actionChecker.endAction(applyAction);
        }
      } catch (Exception e) {
        LOG.error(tabletInfo.getTabletName() + ":" + e.getMessage(), e);
      }
      latch.exit(txId.getTxId());
    }
  }

  public void clearCommitLogErrorState() {
    if (inCommitLogError.getAndSet(0) > 0) {
      LOG.info("[" + this.tabletInfo.getTabletName() + "] reset commit log error flag after MinorCompaction");
    }
  }

  public void processCommitLogServerFailed() throws IOException {
    LOG.info("TabletName [" + tabletInfo.getTabletName() + 
        "] is set as error state cause by commitLogServer failed.");

    forceToCancelAllTxes();

    if (!this.isSplitting()) {
      forceToRunMinorCompaction();
    }      
  }
  
  public boolean checkCommitLogError() throws IOException {
    if(inCommitLogError.get() == 0) {
      return true;
    }
    if(memorySSTable.commitLogFileSystem.ping(tabletInfo.getTabletName())) {
      inCommitLogError.set(0);
      return true;
    } else {
      return false;
    }
  }

  private boolean mutateMemorySSTable(TxId txId, Row.Key rowKey, 
      CommitLog[] commitLogList,
      boolean saveLog) throws TxException {
    try {
      boolean success = memorySSTable.commit(txId, commitLogList, saveLog);
      if(success && inCommitLogError.getAndSet(0) > 0) {
        LOG.info("[" + this.tabletInfo.getTabletName() + "] reset commit log error flag");
      }
      return success;
    } catch (CommitLogInterruptedException e) {
      try {
        doMinorCompaction(null, true);
      } catch (IOException e1) {
        LOG.error(e1);
      }
      LOG.info("[" + tabletInfo.getTabletName()
          + "] Thread is interrupted in mutating. Retry by client");
      return false;
    } catch (CommitLogException e) {
      LOG.warn("[" + this.tabletInfo.getTabletName() + "] CommitLog Error, " + e);
      if(inCommitLogError.incrementAndGet() > MAX_COMMITLOG_ERROR) {
        try {
          doMinorCompaction(null, true);
        } catch (IOException e1) {
          LOG.error(e1);
        }
      }
      return false;
    } catch (Exception e) {
      LOG.error("Error while mutateMemorySSTable:" + tabletInfo, e);
      throw new TxException(tabletServer.getHostName() + ":" + e.getMessage());
    }
  }

  private void forceToCancelAllTxes() {
    Enumeration<TxValue> en = txMap.elements();
    while (en.hasMoreElements()) {
      TxValue value = en.nextElement();
      LOG.info("force to cancel thread [" + value.txOwnerThread + "]");
      value.txOwnerThread.interrupt();
    }
  }

  private void forceToRunMinorCompaction() {
    LOG.info("force to run minor compaction:" + tabletInfo.getTabletName());

    Thread minorScheduleThread = new Thread() {
      public void run() {
        synchronized(forceMinorCompactionLock) {
          if(forceMinorCompactionLock.get()) {
            LOG.info("Already called forceToRunMinorCompaction:" + tabletInfo.getTabletName());
            return;
          } else {
            forceMinorCompactionLock.set(true);
          }
        } 
        
        try {
          int retryCount = 0;
          
          minorCompactionAction.setForced(true);
          
          while (retryCount++ < 100) {
            if (actionChecker.startAction(minorCompactionAction)) {
              break;
            }
  
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e1) {
              return;
            }
          }
  
          if (retryCount >= 100) {
            List<String> actionList = actionChecker.getAllActions();
  
            for (String actionInfo : actionList) {
              LOG.fatal(actionInfo);
            }
  
            LOG.fatal("Kill TabletServer due to failure of starting minorCompaction");
            System.exit(-1);
          }
        } finally {
          synchronized(forceMinorCompactionLock) {
            forceMinorCompactionLock.set(false);
          }
        }
      }
    };
    
    minorScheduleThread.start();
  }

  /**
   * META/ROOT tablet에 split된 tablet 정보를 저장한다. 3개의 작업(1 delete, 2 insert)이 하나의
   * commit log에 저장되도록한다.
   * 
   * @param txId
   * @param splitedTablets
   * @throws IOException
   */
  public void saveTabletSplitedInfo(TxId txId, TabletInfo targetTablet, TabletInfo[] splitedTablets)
      throws IOException {
    TxValue txValue = getTxValue(txId);

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(bout);
    targetTablet.write(out);
    for (int i = 0; i < splitedTablets.length; i++) {
      splitedTablets[i].write(out);
    }
    CommitLog commitLog = new CommitLog(Constants.LOG_OP_MODIFY_META, tabletInfo.getTableName(),
        txValue.getRowKey(), META_COLUMN_NAME_TABLETINFO, new Cell.Key(""), System
            .currentTimeMillis(), bout.toByteArray());
    txValue.addCommitLog(commitLog);
  }

  /**
   * MemoryTablet이 일정 사이즈보다 커지게 되면 Memory의 내용을 Disk의 MapFile로 저장한다.
   * 
   * @throws IOException
   */
  public void doMinorCompaction() throws IOException {
    doMinorCompaction(null, false);
  }

  public void doMinorCompaction(boolean forced) throws IOException {
    doMinorCompaction(null, forced);
  }
  
  public void doMinorCompaction(ActionFinalizer finalizer) throws IOException {
    doMinorCompaction(finalizer, false);
  }
  
  public void doMinorCompaction(ActionFinalizer finalizer, boolean forced) throws IOException {
    if(conf.getBoolean("tabletServer.noMinorCompaction", false)) {
      return;
    }
    minorCompactionAction.setForced(forced);
    minorCompactionAction.setFinalizer(finalizer);
    actionChecker.startAction(minorCompactionAction);
  }

  public boolean needMajorCompaction(int mapFileCount) {
    //LOG.debug("TableMapFileCount:" + tabletInfo.getTabletName() + ":" + getMapFileCount() + "," + mapFileCount);
    return getMapFileCount() > mapFileCount;
  }

  public boolean isDroped() {
    return this.droped;
  }

  protected void clearAll() throws IOException {
    for (Map.Entry<String, TabletScanner> entry : tabletScanners.entrySet()) {
      try {
        ScannerAction action = new ScannerAction(entry.getKey());
        actionChecker.endAction(action);
        entry.getValue().close();
      } catch (IOException e) {
        LOG.error("Can't close scanner while drop:" + tabletInfo, e);
      }
    }

    actionChecker.endActions();
    // TODO Tablet이 compaction 중인 경우에는 어떻게 처리해야 하나? -> drop이 호출되기전에 모두 종료된 상태임

    tabletScanners.clear();

    droped = true;
    ready.set(false);

    tabletServer.removeFromMemoryCache(tabletInfo.getTabletName(), diskSSTable.get().getColumnMemoryCachesSize());
    
    // memory 정보 삭제
    memorySSTable.clearAllMemory();
    diskSSTable.get().clearAllMemory();

    lockedRow.clear();
    txMap.clear();
  }

  public void drop() throws IOException {
    clearAll();

    // commit log 삭제
    deleteCommitLog();
  }

  public boolean doStop() {
    blockLatch();
    TabletLockAction lockAciton = new TabletLockAction();
    try {
      return actionChecker.startAction(lockAciton);
    } finally {
      unblockLatch();
    }
  }
  
  /**
   * 하나의 Tablet에서 관리하고 있는 여러개의 MapFile을 merge하여 하나의 MapFile로 만든다.
   * 
   * @throws IOException
   */

  public void doMajorCompaction(ActionFinalizer finalizer) throws IOException {
    majorCompactionAction.setFinalizer(finalizer);
    if (!actionChecker.startAction(majorCompactionAction)) {
      return;
    }
  }

  public void doMajorCompaction() throws IOException {
    doMajorCompaction(null);
  }

  public void doTabletSplit() throws IOException {
    doTabletSplit(null);
  }

  public void doTabletSplit(ActionFinalizer finalizer) {
    if(!actionChecker.hasAlreadyScheduled(tabletSplitAction)) { 
      tabletSplitAction.setFinalizer(finalizer);
      actionChecker.startAction(tabletSplitAction);
    }
  }
  
  public boolean needToBeSplited() throws IOException {
    return neededSplit(splitRatioConstants) &&
      !actionChecker.hasAlreadyScheduled(tabletSplitAction);
  }

  public boolean neededSplit(double checkRate) throws IOException {
    if(conf.getBoolean("tabletServer.noSplit", false)) {
      return false;
    }
    // Max Disk 크기의 80%가 되면 split 수행
    // Memory의 경우 실제 Class 오버헤더가 있기 때문에 30%만 적용
    if(spliting) {
      return false;
    }
    return getTabletSize() >= (maxTabletSizeByte * checkRate);
  }

  public double getTabletSize() {
    try {
      long mapFileSize = sumMapFileSize();
      if (mapFileSize < 0) {
        mapFileSize = 0;
      }

      long memorySize = memorySSTable.getTabletSize();

      return (mapFileSize + memorySize * 0.3);
    } catch (Exception e) {
      LOG.error(tabletInfo.getTabletName() + ": " + e.getMessage(), e);
      return -1;
    }
  }

  public int getMapFileCount() {
    return diskSSTable.get().getMaxMapFileCount();
  }

  public long sumMapFileSize() throws IOException {
    return diskSSTable.get().sumMapFileSize();
  }

  /**
   * Client API에서는 ColumnRecord 하나만 반환하도록 하지만 네트워크 연결 횟수를 줄이기 위해 한번에 fetch하는 사이즈
   * 만큼 전송한다.
   * 
   * @param scannerId
   * @return
   * @throws IOException
   */
  public ColumnValue[] scanNext(String scannerId) throws IOException {
    // lock.obtainReadLock();
    TabletScanner tabletScanner = null;
    try {
      tabletScanner = tabletScanners.get(scannerId);
      if (tabletScanner == null) {
        throw new IOException("[" + scannerId + "] not opened");
      }
    } finally {
      // lock.releaseReadLock();
    }
    return tabletScanner.next();
  }

  
  /**
   * get의 경우 호출될 때마다 매번 MapFile을 open한 후 seek 해야 하기 때문에 대량의 데이터를 sequential하게
   * read하는데에는 부적합하다. Scanner의 경우 scanner를 open할 때 MapFile을 열어 seek 한 후 next()
   * 호출에 의해 파일 포인터를 이동시키면서 데이터를 조회한다.
   * 
   * Scanner는 Minor, Major compaction, tablet split, merge 중인 경우에는 open할 수 없다. 이
   * 경우 바로 null을 반환하고 클라이언트 모듈(Cloudata Client)에서 여러번 retry하도록 한다. 이유는 Split 되는 경우
   * Spilit 종료 후에는 해당 Row Range에 대한 Tablet Server가 변경되어 있기 때문이다.
   * 
   * @param startRowKey
   * @param endRowKey
   * @param columnNames
   * @return
   * @throws IOException
   */
  public TabletScanner openScanner(String columnName, RowFilter scanFilter) throws IOException {
    return openScanner(columnName, scanFilter, true);
  }
  
  public TabletScanner openScanner(String columnName, RowFilter rowFilter, boolean checkAction) throws IOException {
    String scannerId = TabletScanner.generateScannerId(tabletInfo);

    ScannerAction action = null;
    if(checkAction) {
      action = new ScannerAction(scannerId);

      // Minor, Major Compaction 중인 경우 open하지 않고 return
      if (!actionChecker.startAction(action)) {
        LOG.info(tabletInfo.getTabletName()
            + ": Can't open scanner while TabletStop/Compaction/Split running");
        return null;
      }
    }

    try {
      CellFilter cellFilter = null;
      if (rowFilter.getCellFilters().size() > 0) {
        for (CellFilter eachCellFilter : rowFilter.getCellFilters()) {
          if (columnName.equals(eachCellFilter.getColumnName())) {
            cellFilter = eachCellFilter;
            break;
          }
        }
      }

      if (cellFilter == null) {
        cellFilter = new CellFilter(columnName);
      }

      TabletScanner tabletScanner = new TabletScanner(scannerId, conf, tabletInfo, rowFilter
          .getStartRowKey(), rowFilter.getEndRowKey(), columnName, memorySSTable, diskSSTable
          .get(), cellFilter, table.getNumOfVersion(), checkAction);

      tabletScanners.put(tabletScanner.getScannerId(), tabletScanner);
      return tabletScanner;
    } catch (Exception e) {
      e.printStackTrace();
      if(checkAction) {
        actionChecker.endAction(action);
      }
      throw new IOException("Error open scanner:columnName=" + columnName + " :" + e.getMessage(), e);
    }
  }

  /**
   * open되어 있는 scanner를 close한다.
   * 
   * @param scannerId
   * @throws IOException
   */
  public void closeScanner(String scannerId) throws IOException {
    closeScanner(scannerId, true);
  }
  
  public void closeScanner(String scannerId, boolean checkAction) throws IOException {
    ScannerAction action = null;
    if(checkAction) {
      action = new ScannerAction(scannerId);
    }

    TabletScanner tabletScanner = tabletScanners.get(scannerId);
    if (tabletScanner == null) {
      if(checkAction) {
        actionChecker.endAction(action);
      }
      throw new IOException("[" + scannerId + "] not opened");
    }
    tabletScanner.close();
    tabletScanners.remove(scannerId);

    if(checkAction) {
      actionChecker.endAction(action);
    }
  }

  public TabletInfo getTabletInfo() {
    return tabletInfo;
  }

  /**
   * 
   * @return
   */
  public int getOpenedScannerCount() {
    // lock.obtainReadLock();
    try {
      return tabletScanners.size();
    } finally {
      // lock.releaseReadLock();
    }
  }

  /**
   * 
   * @return
   */
  public TabletReport getTabletDetailInfo() throws IOException {
    TabletReport tabletReport = new TabletReport();
    tabletReport.setTabletInfo(tabletInfo);

    tabletReport.setRunningActions(actionChecker.getRunningActions().toArray(new String[] {}));
    tabletReport.setWaitingActions(actionChecker.getWaitingActions().toArray(new String[] {}));

    tabletReport.setMemoryTabletSize(memorySSTable.getTabletSize());
    tabletReport.setMemoryTabletDataCount(memorySSTable.getDataCount());

    tabletReport.setMapFilePaths(diskSSTable.get().getMapFilePathList());
    tabletReport.setColumnMapFileCount(diskSSTable.get().getMapFileCount());
    tabletReport.setColumnMapFileSize(diskSSTable.get().getMapFileSize());
    tabletReport.setColumnIndexSize(diskSSTable.get().getMapFileIndexSize());

    tabletReport.setServerMaxMemory(Runtime.getRuntime().maxMemory());
    tabletReport.setServerFreeMemory(Runtime.getRuntime().freeMemory());
    tabletReport.setServerTotalMemory(Runtime.getRuntime().totalMemory());

    tabletReport.setCommitLogServers(memorySSTable.getCommitLogServerInfo());
    return tabletReport;
  }

  /**
   * 
   * @throws IOException
   */
  public void deleteTablet() throws IOException {
    clearAll();

    try {
      // delete tablet data
      GPath dataPath = Tablet.getTabletPath(conf, tabletInfo);
      long startTime = System.currentTimeMillis();
      while (true) {
        boolean result = fs.delete(dataPath, true);
        if (result) {
          break;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
        if (System.currentTimeMillis() - startTime > 30 * 1000) {
          LOG.warn("Can't delete " + dataPath + " while droping " + tabletInfo);
          break;
        }
      }
    } catch (IOException e) {
      LOG.error("found exception while deleting data file file for drop", e);
      e.printStackTrace(System.out);
      throw e;
    }

    // delete tablet commit log
    try {
      CommitLogFileSystemIF clogFs = memorySSTable.getCommitLogFileSystem();
      if (clogFs != null) {
        clogFs.delete(tabletInfo.getTabletName());
        clogFs.close(tabletInfo.getTabletName(), true);
      }
    } catch (IOException e) {
      LOG.error("found exception while deleting commit log file for drop", e);
      throw e;
    }
  }

  public void deleteCommitLog() throws IOException {
    deleteCommitLog(this.memorySSTable.getCommitLogFileSystem(), tabletInfo.getTableName(),
        tabletInfo.getTabletName());
  }

  public static void deleteCommitLog(CommitLogFileSystemIF commitLogFileSystem, String tableName,
      String tabletName) throws IOException {
    // TODO CommitLog도 삭제하지 않고 Trash로 이동
    if (commitLogFileSystem == null) {
      return;
    }
    try {
      if (commitLogFileSystem.exists(tabletName)) {
        commitLogFileSystem.delete(tabletName);
      }
    } catch (IOException e) {
      LOG.error("Can't delete CommitLog file while drop:" + e.getMessage(), e);
      throw e;
    }
  }

  public AtomicBoolean getReady() {
    return ready;
  }

  /**
   * 
   * @return
   */
  public boolean isReady() {
    return ready.get();
  }

  public void deleteSplitTempDir() throws IOException {
    fs.delete(Tablet.getTabletSplitTempPath(conf, tabletInfo), true);
  }

  public void truncateColumn(String columnName) throws IOException {
    if (memorySSTable.getTabletSize() > 0) {
      doMinorCompaction();
      while (true) {
        try {
          Thread.sleep(1 * 1000);
        } catch (InterruptedException e) {
        }
        if (memorySSTable.getTabletSize() == 0) {
          break;
        }
      }
    }
    diskSSTable.get().truncateColumn(columnName);
  }

  public void addColumn(String addedColumnName) throws IOException {
    synchronized (table) {
      if (!table.getColumns().contains(addedColumnName)) {
        table.addColumn(addedColumnName);
      }
    }
  }

  public DiskSSTable getDiskSSTable() {
    return diskSSTable.get();
  }

  public MemorySSTable getMemorySSTable() {
    return this.memorySSTable;
  }

  public ActionChecker getActionChecker() {
    return this.actionChecker;
  }

  public String[] getAllActions() {
    List<String> actions = actionChecker.getAllActions();
    return actions.toArray(new String[] {});
  }

  public boolean isEmpty() {
    return memorySSTable.isEmpty() && diskSSTable.get().isEmpty();
  }

  public boolean hasValue(String columnName, Row.Key rowKey, Cell.Key cellKey) throws IOException {
    RecordSearcher recordSearcher = new RecordSearcher(this);
    return recordSearcher.hasValue(columnName, rowKey, cellKey);
  }

  public static Row.Key generateMetaRowKey(String tableName, Row.Key rowKey) {
    if (tableName == null) {
      return Row.Key.MAX_KEY;
    }

    byte[] tableNameBytes = tableName.getBytes();
    byte[] metaRowKeyBytes = new byte[tableNameBytes.length + rowKey.getLength() + 1];
    System.arraycopy(tableNameBytes, 0, metaRowKeyBytes, 0, tableNameBytes.length);
    metaRowKeyBytes[tableNameBytes.length] = (byte) '.';
    System.arraycopy(rowKey.getBytes(), 0, metaRowKeyBytes, tableNameBytes.length + 1, rowKey
        .getLength());

    return new Row.Key(metaRowKeyBytes);
  }


  public static TabletInfo getRootTabletInfo(CloudataConf conf, ZooKeeper zk)
      throws IOException {
    byte[] data = null;
    try {
      data = zk.getData(LockUtil.getZKPath(conf,Constants.ROOT_TABLET), false, null);
    } catch (NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    if (data == null) {
      LOG.warn(Constants.ROOT_TABLET + " is null"
          + ", so ROOT tablet info is also null");
      return null;
    }
    
    TabletInfo rootTabletInfo = new TabletInfo();
    
    rootTabletInfo.readFields(data);
    rootTabletInfo.setAssignedHostName(null);
    
    data = null;
    try {
      data = zk.getData(LockUtil.getZKPath(conf, Constants.ROOT_TABLET_HOST), false, null);
    } catch (NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    if(data != null) {
      rootTabletInfo.setAssignedHostName(new String(data));
    } 
    
    return rootTabletInfo;
  }

  public static GPath getTabletPath(CloudataConf conf, TabletInfo tabletInfo) {
    return new GPath(TableSchema.getTableDataPath(conf, tabletInfo.getTableName()) + "/"
        + tabletInfo.getTabletName());
  }

  public static GPath getTabletPath(CloudataConf conf, String tableName, String tabletName) {
    return new GPath(TableSchema.getTableDataPath(conf, tableName) + "/" + tabletName);
  }

  public static String generateTabletName(String tableName) {
    return tableName + "_" + IdGenerator.getId();
  }

  public static GPath getTabletSplitTempPath(CloudataConf conf, String tableName,
      String tabletName) {
    return new GPath(getTabletPath(conf, tableName, tabletName) + DIR_TEMP_SPLIT);
  }

  public static GPath getTabletSplitTempPath(CloudataConf conf, TabletInfo tabletInfo) {
    return new GPath(getTabletPath(conf, tabletInfo) + DIR_TEMP_SPLIT);
  }

  public static GPath getTabletMajorCompactionTempPath(CloudataConf conf, String tableName,
      String tabletName) {
    return new GPath(getTabletPath(conf, tableName, tabletName) + DIR_TEMP_MAJOR);
  }

  public static GPath getTabletMajorCompactionTempPath(CloudataConf conf, TabletInfo tabletInfo) {
    return new GPath(getTabletPath(conf, tabletInfo) + DIR_TEMP_MAJOR);
  }

  public static GPath getTabletMinorCompactionTempPath(CloudataConf conf, String tableName,
      String tabletName) {
    return new GPath(getTabletPath(conf, tableName, tabletName) + DIR_TEMP_MINOR);
  }

  public static GPath getTabletMinorCompactionTempPath(CloudataConf conf, TabletInfo tabletInfo) {
    return new GPath(getTabletPath(conf, tabletInfo) + DIR_TEMP_MINOR);
  }

  public static GPath getTabletLocalTempPath(CloudataConf conf, TabletInfo tabletInfo,
      String actionId) {
    String tabletLocalTempPath = conf.get("cloudata.local.temp") + "/" + actionId;

    return new GPath(tabletLocalTempPath + DIR_TEMP_UPLOAD);
  }

  public static GPath getTabletUploadTempPath(CloudataConf conf, TabletInfo tabletInfo,
      String actionId) {
    return new GPath(getTabletPath(conf, tabletInfo) + DIR_TEMP_UPLOAD + "/" + actionId);
  }

  public static String getTableNameFromTabletName(String tabletName) {
    // FIXME 나중에 Meta 테이블에 Table명 저장하는 형태로 변경
    int index = tabletName.lastIndexOf("_");
    if (index < 0)
      return null;
    return tabletName.substring(0, index);
  }

  public TabletServer getTabletServer() {
    return tabletServer;
  }

  public Row.Key[] getSplitedRowKeyRanges(int splitPerTablet) throws IOException {
    if (splitPerTablet < 0) {
      return null;
    }

    SortedSet<Row.Key> memoryRowKeys = memorySSTable.getAllRowKeys();
    SortedSet<MapFileIndexRecord> indexRecords = diskSSTable.get().getMapFileIndex();

    SortedSet<Row.Key> rowKeys = null;

    int memoryRowKeySize = memoryRowKeys.size();
    int indexSize = indexRecords.size();
    if (indexSize > 0 && memoryRowKeySize > indexSize * 2) {
      rowKeys = new TreeSet<Row.Key>();
      int gap = memoryRowKeySize / indexSize;

      Row.Key[] rowKeyArr = new Row.Key[memoryRowKeySize];
      memoryRowKeys.toArray(rowKeyArr);

      for (int i = 0; i < indexSize; i++) {
        rowKeys.add(rowKeyArr[(i + 1) * gap - 1]);
      }
    } else {
      rowKeys = memoryRowKeys;
    }

    for (MapFileIndexRecord eachIndex : indexRecords) {
      rowKeys.add(eachIndex.getRowKey());
    }

    int rowKeySize = rowKeys.size();

    if (splitPerTablet >= rowKeySize) {
      return rowKeys.toArray(new Row.Key[] {});
    } else {
      Row.Key[] result = new Row.Key[splitPerTablet];

      Row.Key[] rowKeyArr = new Row.Key[rowKeySize];
      rowKeys.toArray(rowKeyArr);
      int gap = rowKeySize / splitPerTablet;
      for (int i = 0; i < splitPerTablet - 1; i++) {
        result[i] = rowKeyArr[(i + 1) * gap];
      }
      result[splitPerTablet - 1] = tabletInfo.getEndRowKey();

      return result;
    }
  }

  public void startSplit() {
    spliting = true;
  }
  
  public void endSplit() {
    spliting = false;
  }
  
  public boolean isSplitting() {
    //return actionChecker.hasAlreadyScheduled(tabletSplitAction);
    return spliting;
  }

  public boolean needRegulation() {
    return tabletServer.isInIntensivePut();
  }

  public void setMinorCompactionTime(long bytesPerMilliSec) {
    tabletServer.setLastMinorCompactionTime(bytesPerMilliSec);
  }
}
