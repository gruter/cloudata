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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.commitlog.CommitLogInterruptedException;
import org.cloudata.core.commitlog.CommitLogOverLoadedException;
import org.cloudata.core.commitlog.CommitLogStatus;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.IdGenerator;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.CommitLogException;
import org.cloudata.core.fs.CommitLogFileSystem;
import org.cloudata.core.fs.CommitLogFileSystemIF;
import org.cloudata.core.fs.CommitLogLoader;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.metrics.TabletServerMetrics;


public class MemorySSTable {
  public static final Log LOG = LogFactory.getLog(MemorySSTable.class.getName());

  protected TabletInfo tabletInfo;

  protected Tablet tablet;
  
  protected int tabletSize;

  protected AtomicInteger tabletSizeBeforeCompaction = new AtomicInteger(0);

  // ColumnName, ColumnCollection
  protected final ReentrantReadWriteLock columnCollectionsLock
    = new ReentrantReadWriteLock();
  protected HashMap<String, ColumnCollection> columnCollections 
    = new HashMap<String, ColumnCollection>(10);

  protected HashMap<String, ColumnCollection> compactingColumnCollections;

//  protected boolean compacting = false;

  protected CloudataFileSystem fs;

  protected CloudataConf conf;

  protected CommitLogFileSystemIF commitLogFileSystem;

  protected TabletServer tabletServer;

  private int numOfVersion;
  
  AtomicBoolean inBackupCommitLogStatus = new AtomicBoolean(false);
  
  AtomicInteger numOfCommitThreads = new AtomicInteger(0);

  private boolean commitLogTest = false;
  
  private int commitLogSeq;
  
  public MemorySSTable() {
  }

  public void init(TabletServer tabletServer, CloudataConf conf, Tablet tablet,
      int numOfVersion) throws IOException {
    this.tabletServer = tabletServer;
    this.tabletInfo = tablet.getTabletInfo();
    this.tablet = tablet;
    this.numOfVersion = numOfVersion;
    this.conf = conf;
    this.fs = CloudataFileSystem.get(conf);
    if (tabletServer != null) {
      try {
        this.commitLogFileSystem = CommitLogFileSystem.getCommitLogFileSystem(conf, tabletServer, tabletServer.getZooKeeper());
      } catch (Exception e) {
        LOG.fatal("TabletServer is shut down due to fail initializing commitlog filesystem", e);
        tabletServer.shutdown();
      }
    }

    this.compactingColumnCollections = new HashMap<String, ColumnCollection>(10);
    
    commitLogTest = conf.getBoolean("commitlog.num.test.replicas", false);
  }

  public String[] getCommitLogServerInfo() throws IOException {
    InetSocketAddress[] addresses = commitLogFileSystem.getCommitLogServerInfo(tabletInfo.getTabletName());
    
    if(addresses == null || addresses.length == 0) {
      return new String[]{};
    }
    
    String[] addrs = new String[addresses.length];
    for(int i = 0; i < addresses.length; i++) {
      addrs[i] =  addresses[i].getHostName() + ":" + addresses[i].getPort();
    }
    
    return addrs;
  }
  
  public void clearAllMemory() {
    columnCollectionsLock.writeLock().lock();
    try {
      columnCollections.clear();
      if (compactingColumnCollections != null) {
        compactingColumnCollections.clear();
      }
    } finally {
      columnCollectionsLock.writeLock().unlock();
    }
  }

  public Row.Key findMidRowKeyForSplit() throws IOException {
    int max = 0;
    ColumnCollection maxColumnCollection = null;
    columnCollectionsLock.readLock().lock();
    try {
      for (Map.Entry<String, ColumnCollection> entry : columnCollections.entrySet()) {
        ColumnCollection columnCollection = entry.getValue();
        if (columnCollection.getRowCount() > max) {
          max = columnCollection.getRowCount();
          maxColumnCollection = columnCollection;
        }
      }
    } finally {
      columnCollectionsLock.readLock().unlock();
    }

    if (maxColumnCollection == null) {
      return null;
    } 
    
    return maxColumnCollection.getMidRowKey();
  }

  public SortedSet<Row.Key> getAllRowKeys() {
    columnCollectionsLock.readLock().lock();
    try {
      SortedSet<Row.Key> result = new TreeSet<Row.Key>();
      for (Map.Entry<String, ColumnCollection> entry : columnCollections.entrySet()) {
        ColumnCollection columnCollection = entry.getValue();
        result.addAll(columnCollection.getRowKeySet());
      }
      return result;
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
  }

  public void print() {
    columnCollectionsLock.readLock().lock();
    try {
      for (Map.Entry<String, ColumnCollection> entry : columnCollections.entrySet()) {
        System.out.println("Column:" + entry.getKey());
        entry.getValue().print();
      }
      for (Map.Entry<String, ColumnCollection> entry : compactingColumnCollections.entrySet()) {
        System.out.println("Column:" + entry.getKey());
        entry.getValue().print();
      }
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
  }

  public int getTabletSize() {
    return tabletSize;
  }

  public int[] getDataCount() {
    columnCollectionsLock.readLock().lock();
    try {
      Set<String> columnNames = columnCollections.keySet();

      boolean useCompactingMap = false;
      
      if (columnNames == null || columnNames.isEmpty()) {
        columnNames = compactingColumnCollections.keySet();
        if (columnNames == null || columnNames.isEmpty()) {
          return new int[] { 0 };
        }
        useCompactingMap = true;
      }
  
      int result[] = new int[columnNames.size()];
      int index = 0;
      for (String column : columnNames) {
        ColumnCollection columnCollection = null;
        if(useCompactingMap) {
          columnCollection = compactingColumnCollections.get(column);
        } else {
          columnCollection = columnCollections.get(column);
        }
        if (columnCollection != null) {
          result[index] = columnCollection.columnCollectionMap.size();
        }
        index++;
      }
      return result;
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
  }

  public CommitLogFileSystemIF getCommitLogFileSystem() {
    return commitLogFileSystem;
  }

  // only for test
  public Map<String, ColumnCollection> getColumnCollections() {
    return columnCollections;
  }

  public ColumnValue[] getAllMemoryValues(String columnName) {
    ColumnCollection columnCollection = columnCollections.get(columnName);
    if (columnCollection == null) {
      return null;
    }

    return columnCollection.getAllValues();
  }

  public boolean commit(TxId txId, CommitLog[] commitLogList, boolean saveLog) throws IOException {
    if (saveLog && !commitLogTest) {
      String tabletName = tabletInfo.getTabletName();
      String txIdStr = txId.getTxId();

      // open pipe for writing commit logs
      try {
        // by sangchul
        // Exception Lists
        // CommitLogOverLoadedException : the total number of pipes exceeds max value  
        // CommitLogInterruptedException : Threads get interrupted while IO operation
        // CommitLogException : an error occurs while opening pipe for writing commit logs
        // , causing minor compaction
        // IOException
        commitLogFileSystem.open(tabletName, true);
      } catch (CommitLogOverLoadedException e) {
        // force client to try again later.
        return false;
      } catch (CommitLogInterruptedException e) {
        throw e;
      } catch (IOException e) {
        throw new CommitLogException(e);
      }

      // writing commit logs
      // by sangchul
      // CommitLogInterruptedException : Threads get interrupted while IO operation
      // IOException
      try {
        //int index = 1;
        for (CommitLog commitLog : commitLogList) {
          commitLogFileSystem.addCommitLog(tabletName, txIdStr, commitLogSeq, commitLog);
        }
        commitLogFileSystem.finishAdding(tabletName, txIdStr);
        commitLogSeq++;
      } catch (CommitLogInterruptedException e) {
        throw e;
      } catch (IOException e) {
        LOG.error("Commit Log Error in tablet [" + tabletInfo.getTabletName() + "], txid ["
            + txId + "], exception : " + e);
        throw new CommitLogException(e);
      }
    }
    
    // apply changes to memory
    try {
      for (CommitLog commitLog : commitLogList) {
        addValue(commitLog);
      }
    } catch (Exception e) {
      // TODO how to handle already written commit logs in this case?
      LOG.error("Exception in applying changes in memory", e);
      throw new IOException(e.getMessage());
    }
    
    return true;
  }

  public void clearCommitLog() throws IOException {
    if(conf.getBoolean("tabletServer.noMinorCompatcion", false)) {
      return;
    }

    LOG.info(tabletInfo.getTabletName() + " clear commit log info");
    try {
      commitLogFileSystem.delete(tabletInfo.getTabletName());
    } catch (IOException e) {
      LOG.warn("commitLogFileSystem.delete error:" + e.getMessage(), e);
    }
    commitLogFileSystem.close(tabletInfo.getTabletName(), false);
  }

  public void endCommitLogMinorCompaction() throws IOException {
    int retry = 1;
    while(true) {
      try {
        commitLogFileSystem.endMinorCompaction(tabletInfo.getTabletName());
        return;
      } catch (CommitLogOverLoadedException e) {
        retry++;
        if(retry >= 20) {
          throw e;
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException err) {
          return;
        }
        LOG.info("occur CommitLogOverLoadedException while endCommitLogMinorCompaction, but retry:" + retry);
      }
    }
  }

  /**
   * false가 return되면 로딩 후 바로 minorcompaction을 수행해야 한다.
   */
  public boolean loadFromCommitLog() throws IOException {
    if (commitLogFileSystem == null) {
      return true;
    }
    String tabletName = tabletInfo.getTabletName();

    if (!commitLogFileSystem.exists(tabletName)) {
      return true;
    }

    CommitLogStatus commitLogStatus = commitLogFileSystem.verifyCommitlog(tabletName);
    if (commitLogStatus == null) {
      throw new IOException(tabletName + " error while Verify Commitlog");
    }

    if(commitLogStatus.isNeedCompaction()) {
      LOG.info(tabletName + " different commit log file size");
      if(Constants.TABLE_NAME_ROOT.equals(tabletInfo.getTableName()) ||
              Constants.TABLE_NAME_META.equals(tabletInfo.getTableName())) {
          throw new IOException(tabletName + " different commit log file size");
      }
    }
    
    try {
      CommitLog commitLog = new CommitLog();
      int count = 0;

      CommitLogLoader loader = commitLogFileSystem.getCommitLogLoader(tabletName, commitLogStatus);
      while ((commitLog = loader.nextCommitLog()) != null) {
        addValue(commitLog);
        count++;
      }
      commitLogSeq = loader.getLastSeq() + 1;
      if(!loader.isMatchLastSeq()) {
        commitLogStatus.setNeedCompaction(true);
      }
      LOG.info(tabletName + " " + count + " commit logs are loaded, commitLogSeq:" + commitLogSeq);
    } catch (IOException e) {
      // FIXME Commitlog 파일에 문제가 있는 경우 전체 시스템에 경고를 주고, 매뉴얼하게 fsck하는 기능이 있어야 한다.
      LOG.error("Check commit log file:" + tabletName, e);
      throw e;
    } catch (Exception e) {
      LOG.error("Check commit log file:" + tabletName, e);
      throw new IOException(e);
    }

    if (commitLogStatus.isNeedCompaction()) {
      return false;
    } else {
      return true;
    }
  }

  private void addValue(CommitLog commitLog) throws IOException {
    if (commitLog.getOperation() == Constants.LOG_OP_MODIFY_META) {
      addMetaValue(commitLog);
    } else {
      String columnName = commitLog.getColumnName();
      columnCollectionsLock.writeLock().lock();
      try {
        ColumnCollection columnCollection = getColumnCollectionIfAbsentMake(columnName); 
          
        boolean deleted = (commitLog.getOperation() == Constants.LOG_OP_DELETE_COLUMN_VALUE);
  
        ColumnValue columnValue = new ColumnValue(commitLog.getRowKey(), commitLog.getCellKey(),
            commitLog.getValue(), deleted, commitLog.getTimestamp());
  
        tabletSize += columnCollection.addValue(commitLog.getRowKey(), columnValue, numOfVersion);
        
        if(commitLog.getValue() != null) {
          tabletServer.tabletServerMetrics.addTxByte(TabletServerMetrics.TYPE_PUT, commitLog.getValue().length);
        }
      } finally {
        columnCollectionsLock.writeLock().unlock();
      }
    }
  }

  /**
   * commit log가 splited된 tablet에 대한 meta 데이터 변경 작업인 경우
   * 
   * @param commitLog
   */
  private void addMetaValue(CommitLog commitLog) throws IOException {
    TabletInfo targetTablet = null;
    DataInput in = null;
    
    TabletInfo[] splitedTabletInfos = new TabletInfo[2];
    try {
      targetTablet = new TabletInfo();
      in = new DataInputStream(new ByteArrayInputStream(commitLog.getValue()));
      targetTablet.readFields(in);
      
      for (int i = 0; i < 2; i++) {
        splitedTabletInfos[i] = new TabletInfo();
        splitedTabletInfos[i].readFields(in);
      }      
    } catch (EOFException e) {
      //1.3 version
      targetTablet = new TabletInfo();
      in = new DataInputStream(new ByteArrayInputStream(commitLog.getValue()));
      targetTablet.readOldFields(in);
      
      for (int i = 0; i < 2; i++) {
        splitedTabletInfos[i] = new TabletInfo();
        splitedTabletInfos[i].readOldFields(in);
      }    
    }
    
    long timestamp = commitLog.getTimestamp();

    Row.Key rowKey = commitLog.getRowKey();

    // delete split target tablet
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    targetTablet.write(new DataOutputStream(bout));

    ColumnValue columnValue = new ColumnValue(rowKey, new Cell.Key(targetTablet.getTabletName()),
        bout.toByteArray(), true, timestamp);

    columnCollectionsLock.writeLock().lock();
    try {
      ColumnCollection columnCollection 
        = getColumnCollectionIfAbsentMake(Constants.META_COLUMN_NAME_TABLETINFO);
      
      columnCollection.addValue(rowKey, columnValue, numOfVersion);
  
      // insert splited tablet
      for (int i = 0; i < 2; i++) {
        timestamp++;
  
        rowKey = Tablet.generateMetaRowKey(splitedTabletInfos[i].getTableName(),
            splitedTabletInfos[i].getEndRowKey());
  
        bout = new ByteArrayOutputStream();
        splitedTabletInfos[i].write(new DataOutputStream(bout));
  
        columnValue = new ColumnValue(rowKey, new Cell.Key(splitedTabletInfos[i].getTabletName()),
            bout.toByteArray(), false, timestamp);
  
        tabletSize += columnCollection.addValue(rowKey, columnValue, numOfVersion);
      }
    } finally {
      columnCollectionsLock.writeLock().unlock();
    }
  }

  private ColumnCollection getColumnCollectionIfAbsentMake(String columnName) {
    ColumnCollection columnCollection = columnCollections.get(columnName);

    if (columnCollection == null) {
      if ((columnCollection = columnCollections.get(columnName)) == null) {
        columnCollection = new ColumnCollection(conf);
        columnCollections.put(columnName, columnCollection);
      }
    }
    return columnCollection;
  }

  public ColumnValue search(Row.Key rowKey, String columnName, Cell.Key cellKey) {
    ColumnCollection columnCollection = null;
    ColumnCollection compactingColumnCollection = null;
    
    columnCollectionsLock.readLock().lock();
    try {
      columnCollection = columnCollections.get(columnName);
      compactingColumnCollection = compactingColumnCollections != null ? 
          compactingColumnCollections.get(columnName) : null;
    } finally {
      columnCollectionsLock.readLock().unlock();
    }

    if (columnCollection != null) {
      ColumnValue columnValue = columnCollection.get(rowKey, cellKey);
      if (columnValue != null) {
        return columnValue.copyColumnValue();
      }
    }

    if (compactingColumnCollection != null) {
      ColumnValue columnValue = compactingColumnCollection.get(rowKey, cellKey);
      if (columnValue != null)
        return columnValue.copyColumnValue();
    }
    
    return null;
  }

  protected List<ColumnValue> searchAllVersion(Row.Key rowKey, String columnName, Cell.Key columnKey)
      throws IOException {
    
    ColumnCollection columnCollection = null;
    ColumnCollection compactingColumnCollection = null;
    columnCollectionsLock.readLock().lock();
    try {
      // columnCollections에 있는 정보가 최신 정보
      columnCollection = columnCollections.get(columnName);
      compactingColumnCollection = compactingColumnCollections != null ?
          compactingColumnCollections.get(columnName) : null;
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
    
    List<ColumnValue> result = new ArrayList<ColumnValue>();

    if (columnCollection != null) {
      columnCollection.searchAllVersion(rowKey, columnKey, result);
    }

    if (compactingColumnCollection != null) {
      compactingColumnCollection.searchAllVersion(rowKey, columnKey, result);
    }

    if (result.isEmpty()) {
      return null;
    } else {
      return result;
    }
  }

  /**
   * 결과는 순서로 정렬되어 있지 않다. Tablet의 RecordSearcher에서 정렬한다.
   */
  public Collection<ColumnValue> search(Row.Key rowKey, CellFilter cellFilter) throws IOException {
    String columnName = cellFilter.getColumnName();
    List<ColumnValue> result = new ArrayList<ColumnValue>(10);
    ColumnCollection columnCollection = null;
    ColumnCollection compactingColumnCollection = null;

    columnCollectionsLock.readLock().lock();
    try {
      compactingColumnCollection = compactingColumnCollections != null ? 
          compactingColumnCollections.get(columnName) : null;
      columnCollection = columnCollections.get(columnName);
    } finally {
      columnCollectionsLock.readLock().unlock();
    }

    // compactingColumnCollections, columnCollections 두군데 동일한 Cell.Key값을 가지는
    // 레코드가 있는 경우
    // columnCollections에 있는 값이 최신 값이다.
    // 따라서 columnCollections에 대한 처리를 뒤에 한다.

    if (compactingColumnCollection != null) {
      compactingColumnCollection.search(rowKey, cellFilter, result);
    }

    if (columnCollection != null) {
      columnCollection.search(rowKey, cellFilter, result);
    }

    return result;
  }

  public boolean isDeleted(Row.Key rowKey, String columnName) throws IOException {
    Collection<ColumnValue> memoryValues = search(rowKey, new CellFilter(columnName));
    if (memoryValues.isEmpty()) {
      return false;
    }
    ValueCollection valueCollection = new ValueCollection();
    for (ColumnValue eachColumnValue : memoryValues) {
      valueCollection.add(eachColumnValue, 0);
    }
    if (valueCollection.isEmpty()) {
      return false;
    }
    return valueCollection.get() == null;
  }

  public ColumnValue findClosest(Row.Key rowKey, String columnName) {
    ColumnCollection columnCollection;
    ColumnCollection compactingColumnCollection;
    
    columnCollectionsLock.readLock().lock();
    try {
      columnCollection = columnCollections.get(columnName);
      compactingColumnCollection = compactingColumnCollections != null ? 
          compactingColumnCollections.get(columnName) : null;
    
      ColumnValue result = null;
      if (columnCollection != null) {
        result = columnCollection.findNearestValue(rowKey);
      }
  
      if (compactingColumnCollection != null) {
        ColumnValue otherResult = compactingColumnCollection.findNearestValue(rowKey);
        if (otherResult != null) {
          if (result == null || otherResult.getRowKey().compareTo(result.getRowKey()) < 0) {
            result = otherResult;
          }
        }
      }
  
      return result;
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
  }

  public boolean hasValue(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    if(cellKey == null) {
      Collection<ColumnValue> memoryValues = search(rowKey, new CellFilter(columnName, cellKey));
      if (memoryValues.isEmpty()) {
        return false;
      }
      ValueCollection valueCollection = new ValueCollection();
      for (ColumnValue eachColumnValue : memoryValues) {
        valueCollection.add(eachColumnValue, 3);
      }
      return valueCollection.get() != null;
    } else {
      return search(rowKey, columnName, cellKey) != null;
    }
  }

//  public boolean enterLatch() {
//    try {
//      latch.block();
//      return true;
//    } catch (InterruptedException e) {
//      LOG.info("enterLatch is interrupted");
//      return false;
//    }
//  }
//  
//  public void leaveLatch() {
//    latch.unblock();
//  }
  
  /**
   * 메모리에 있는 내용을 임시버퍼로 저장시킨 다음, 비어 있는 새로운 메모리를 구성한다. 임시버퍼에 저장된 내용을 파일로 저장한다.
   */
  public synchronized void initMemoryForCompaction() throws IOException {
    //LOG.fatal("Start initMemoryForCompaction:" + tabletInfo.getTabletName());
    if(!tablet.blockLatch()) {
      LOG.info(tabletInfo.getTabletName() + " minorCompaction is interrupted in initMemoryForCompaction");
      throw new IOException(tabletInfo.getTabletName() + " minorCompaction is interrupted in initMemoryForCompaction");
    }
    
    try {
      try {
        commitLogFileSystem.startMinorCompaction(tabletInfo.getTabletName());
      } catch (Exception e) {
        LOG.warn("Can't mark minor compaction to CommitLog Server:"  + tabletInfo.getTabletName() + ":" + e.getMessage(), e);
      }
      columnCollectionsLock.writeLock().lock();

      try {
        compactingColumnCollections = columnCollections;
        columnCollections = new HashMap<String, ColumnCollection>(10);
        tabletSizeBeforeCompaction.set(tabletSize);
        tabletSize = 0;
        //compacting = true;
      } finally {
        columnCollectionsLock.writeLock().unlock();
      }
    } finally {
      tablet.unblockLatch();
    }
  }

  /**
   * Compaction 수행 중 오류가 발생했을 때 메모리를 원상복구 시킨다.
   * 
   */
  public synchronized void cancelCompaction(String fileId) {
    // tempCollections에 저장하는 순서 중요
    // compactingColumnCollections에 저장된 데이터가 더 오래된 버전
    HashMap<String, ColumnCollection> tempCollections = compactingColumnCollections;

    columnCollectionsLock.writeLock().lock();
    try {
      for (Map.Entry<String, ColumnCollection> entry : columnCollections.entrySet()) {
        String columnName = entry.getKey();
        ColumnCollection entryColumnCollection = entry.getValue();
        ColumnCollection tempColumnCollection = tempCollections.get(columnName);
        if (tempColumnCollection == null) {
          tempCollections.put(columnName, entryColumnCollection);
        }
      }
    
      columnCollections = tempCollections;
      compactingColumnCollections.clear();

      tabletSize = tabletSize + tabletSizeBeforeCompaction.getAndSet(0);

      if (fileId != null) {
        // temp 파일 삭제
        for (Map.Entry<String, ColumnCollection> entry : columnCollections.entrySet()) {
          String columnName = entry.getKey();
          GPath tempMapFilePath = new GPath(Tablet.getTabletMinorCompactionTempPath(conf,
              tabletInfo), columnName + "/" + fileId);
          try {
            if (!fs.delete(tempMapFilePath, true)) {
              LOG.error("Can't delete temp minor compaction data while cancel comapction:"
                  + tempMapFilePath);
            }
          } catch (IOException e) {
            LOG.warn("Can't delete temp minor compaction data while cancel comapction:"
                + tempMapFilePath + "," + e.getMessage(), e);
          }
        }
      }
    } finally {
      //compacting = false;
      columnCollectionsLock.writeLock().unlock();
    }
  }

  /**
   * 임시버퍼(compactingColumnCollections)에 저장된 데이터를 파일로 저장한다.
   * 
   * @param tabletInfo
   * @return
   * @throws IOException
   */
  public Map<String, TabletMapFile> saveToDisk(TabletInfo tabletInfo, String fileId)
      throws IOException {
    // compactingColumnCollections에 대한 변경은 Tablet에서 동시에 하나의 Thread만 처리하도록 되어 있기
    // 때문에 동기화 필요 없음
    Map<String, TabletMapFile> result = new HashMap<String, TabletMapFile>(10);

    // Compaction처리 후 생성되는 FileId는 모든 컬럼이 동일하다.
    // 이유는 CommitLog 처리도중 발생하는 오류에 대해 recover하기 위해 commitLog를 임시로 fileId 디렉토리에
    // 이동시킨다.
    for (Map.Entry<String, ColumnCollection> entry : compactingColumnCollections.entrySet()) {
      String columnName = entry.getKey();
      TabletMapFile mapFile = entry.getValue().saveToDisk(tabletInfo, columnName, fileId,
          numOfVersion);
      if (mapFile != null) {
        result.put(columnName, mapFile);
      }
    }
    return result;
  }

  public synchronized void clearCompactingColumnCollections() {
    columnCollectionsLock.writeLock().lock();
    try {
      this.compactingColumnCollections.clear();
      this.tabletSizeBeforeCompaction.set(0);
    } finally {
      //compacting = false;
      columnCollectionsLock.writeLock().unlock();  
    }
  }

  public Scanner getScanner(String columnName, Row.Key startRowKey, Row.Key endRowKey, 
      CellFilter columnFilter, boolean clientSide) throws IOException {
    if(clientSide) {
      return new MemoryScanner(columnName, startRowKey, endRowKey, columnFilter);
    } else {
      return new MemoryServerScanner(columnName, startRowKey, endRowKey, columnFilter);
    }
  }

  public Searchable getSearcher(Row.Key rowKey, CellFilter cellFilter)
      throws IOException {
    return new MemorySearcher(rowKey, cellFilter);
  }

  class MemorySearcher implements Searchable {
    Iterator<ColumnValue> columnValueIt;
    Collection<ColumnValue> columnValues;

    public MemorySearcher(Row.Key rowKey, CellFilter cellFilter) throws IOException {
      this.columnValues = search(rowKey, cellFilter);
      if (columnValues != null) {
        columnValueIt = columnValues.iterator();
      }
    }

    public ColumnValue next() throws IOException {
      if (columnValueIt == null || !columnValueIt.hasNext()) {
        columnValueIt = null;
        return null;
      }

      ColumnValue result = columnValueIt.next();
      return result;
    }

    public void close() throws IOException {
      columnValueIt = null;
      columnValues = null;
    }
  }

  /**
   * TabletServer에서 scanner 생성하여 오픈한 후 데이터를 스캔하여 close 모두 처리
   * 따라서 긴 시간 동안 scanner를 오픈하지 않는다.
   * key set만 가지고 scan
   * 
   * @author babokim
   * 
   */
  class MemoryServerScanner implements Scanner {
    private Row.Key startRowKey;
    
    private Row.Key endRowKey;

    private String columnName;

    private Iterator<ColumnValue> columnValueIt;

    private Iterator<Row.Key> rowKeyIt;

    public MemoryServerScanner(String columnName, Row.Key startRowKey, Row.Key endRowKey,
        CellFilter cellFilter) throws IOException {
      this(columnName, startRowKey, Cell.Key.EMPTY_KEY, endRowKey, cellFilter);
    }

    public MemoryServerScanner(String columnName, Row.Key startRowKey, Cell.Key cellKey,
        Row.Key endRowKey, CellFilter cellFilter) throws IOException {
      this.startRowKey = startRowKey;
      this.endRowKey = endRowKey;
      this.columnName = columnName;

      TreeSet<Row.Key> rowKeys = null;
      columnCollectionsLock.readLock().lock();
      try {
        if (!columnCollections.containsKey(columnName) && !compactingColumnCollections.containsKey(columnName)) {
          return;
        }

        rowKeys = copyRowKeys();
      } finally {
        columnCollectionsLock.readLock().unlock();
      }
      
      try {
        if (rowKeys != null && rowKeys.size() > 0) {
          rowKeyIt = rowKeys.iterator();
          while (rowKeyIt.hasNext()) {
            Row.Key scanRowKey = rowKeyIt.next();
            columnValueIt = initValueIterator(scanRowKey);
            if (columnValueIt.hasNext()) {
              break;
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error init Memory Scanner:" + e.getMessage(), e);
        IOException err = new IOException(e.getMessage());
        err.initCause(e);
        throw err;
      }
    }

    private TreeSet<Row.Key> copyRowKeys() {
      //TODO performance and memory usage 
      TreeSet<Row.Key> rowKeys = new TreeSet<Row.Key>();

      if(columnCollections.get(columnName) != null) {
        SortedMap<Row.Key, SortedMap<Cell.Key, ValueCollection>> targetColumnValues = 
            columnCollections.get(columnName).columnCollectionMap;
        for (Row.Key eachRowKey: targetColumnValues.tailMap(startRowKey).keySet()) {
          if (eachRowKey.compareTo(endRowKey) > 0) {
            break;
          }
          
          rowKeys.add(eachRowKey);
        }
      }

      if(compactingColumnCollections.get(columnName) != null) {
        SortedMap<Row.Key, SortedMap<Cell.Key, ValueCollection>> targetColumnValues = 
            compactingColumnCollections.get(columnName).columnCollectionMap;
        for (Row.Key eachRowKey: targetColumnValues.tailMap(startRowKey).keySet()) {
          if (eachRowKey.compareTo(endRowKey) > 0) {
            break;
          }
          rowKeys.add(eachRowKey);
        }
      }
      
      return rowKeys;
    }

    public ColumnValue next() throws IOException {
      if (columnValueIt == null) {
        return null;
      }

      if (columnValueIt.hasNext()) {
        return columnValueIt.next();
      } else {
        // 현재의 rowKey에 더이상 column 값이 없는 경우, 다음 rowKey 데이터 조회
        while (rowKeyIt.hasNext()) {
          Row.Key scanRowKey = rowKeyIt.next();
          columnValueIt = initValueIterator(scanRowKey);
          if (columnValueIt.hasNext()) {
            return columnValueIt.next();
          }
        }
        return null;
      }
    }
    protected Iterator<ColumnValue> initValueIterator(Row.Key rowKey) throws IOException {
      ColumnCollection columnCollection = null;
      ColumnCollection compactingColumnCollection = null;
      columnCollectionsLock.readLock().lock();
      try {
        // columnCollections에 있는 정보가 최신 정보
        columnCollection = columnCollections.get(columnName);
        compactingColumnCollection = compactingColumnCollections != null ?
            compactingColumnCollections.get(columnName) : null;
      } finally {
        columnCollectionsLock.readLock().unlock();
      }
      
      TreeSet<ColumnValue> result = new TreeSet<ColumnValue>();
    
      if (columnCollection != null) {
        columnCollection.searchAllVersion(rowKey, result);
      }
    
      if (compactingColumnCollection != null) {
        compactingColumnCollection.searchAllVersion(rowKey, result);
      }
    
      return result.iterator();
    }
    
    public void close() throws IOException {
    }
  }
  
  public HashMap<String, ColumnCollection> getCompactingColumnCollections() {
    return compactingColumnCollections;
  }

  /**
   * Scanner는 major, minor compaction 중인 경우에는 생성하지 못한다. 또한 Scanner가 오픈되어 있으면
   * minor, major compaction은 수행하지 않는다. MemoryScanner 객체는 생성 시 scan 범위에 해당하는
   * 데이터만큼 메모리에 복사한다.(메모리 낭비가 심하다-> 튜닝필요) 이렇게 하는 이유는 Scanner가 open된 이후 계속해서
   * 트렌젝션이 들어오는 경우 트렌젝션과 분리시키기 위해서이다.
   * 
   * @author babokim
   * 
   */
  class MemoryScanner implements Scanner {
    private Row.Key startRowKey;
    private Row.Key endRowKey;

    private Cell.Key startCellKey;

    private String columnName;

    private SortedMap<Row.Key, List<ColumnValue>> columnValues;

    private Iterator<Row.Key> rowKeyIt;

    private Iterator<ColumnValue> columnValueIt;

    public MemoryScanner(String columnName, Row.Key startRowKey, Row.Key endRowKey,
        CellFilter cellFilter) throws IOException {
      this(columnName, startRowKey, Cell.Key.EMPTY_KEY, endRowKey, cellFilter);
    }

    public MemoryScanner(String columnName, Row.Key startRowKey, Cell.Key cellKey,
        Row.Key endRowKey, CellFilter cellFilter) throws IOException {
      this.startRowKey = startRowKey;
      this.endRowKey = endRowKey;
      this.columnName = columnName;
      this.startCellKey = cellKey;
      this.columnValues = Collections
          .synchronizedSortedMap(new TreeMap<Row.Key, List<ColumnValue>>());

      columnCollectionsLock.readLock().lock();
      try {
        if (!columnCollections.containsKey(columnName)) {
          return;
        }

        copyValues();
      } finally {
        columnCollectionsLock.readLock().unlock();
      }
      
      try {
        if (columnValues != null && columnValues.size() > 0) {
          rowKeyIt = columnValues.keySet().iterator();
          while (rowKeyIt.hasNext()) {
            Row.Key scanRowKey = rowKeyIt.next();
            List<ColumnValue> list = columnValues.get(scanRowKey);
            if (list != null) {
              columnValueIt = list.iterator();
              if (columnValueIt.hasNext()) {
                break;
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error init Memory Scanner:" + e.getMessage(), e);
        IOException err = new IOException(e.getMessage());
        err.initCause(e);
        throw err;
      }
    }

    private void copyValues() {
      //TODO performance and memory usage 
      SortedMap<Row.Key, SortedMap<Cell.Key, ValueCollection>> targetColumnValues = columnCollections
          .get(columnName).columnCollectionMap;
      
      for (Map.Entry<Row.Key, SortedMap<Cell.Key, ValueCollection>> entry : targetColumnValues
          .tailMap(startRowKey).entrySet()) {
        Row.Key rowKey = entry.getKey();
        if (rowKey.compareTo(endRowKey) > 0) {
          break;
        }
        SortedMap<Cell.Key, ValueCollection> entryValue = entry.getValue();

        List<ColumnValue> rowColumnValues = Collections
            .synchronizedList(new ArrayList<ColumnValue>());
        for (Map.Entry<Cell.Key, ValueCollection> entryValueEntry : entryValue
            .tailMap(startCellKey).entrySet()) {
          if (entryValueEntry.getValue().columnValues != null) {
            rowColumnValues.addAll(entryValueEntry.getValue().columnValues);
          }
        }
        columnValues.put(rowKey, rowColumnValues);
      }
    }

    public ColumnValue next() throws IOException {
      if (columnValues == null || columnValueIt == null) {
        return null;
      }

      if (columnValueIt.hasNext()) {
        return columnValueIt.next();
      } else {
        // 현재의 rowKey에 더이상 column 값이 없는 경우, 다음 rowKey 데이터 조회
        while (rowKeyIt.hasNext()) {
          Row.Key scanRowKey = rowKeyIt.next();
          List<ColumnValue> list = columnValues.get(scanRowKey);
          if (list != null) {
            columnValueIt = list.iterator();
            if (columnValueIt.hasNext()) {
              return columnValueIt.next();
            }
          }
        }
        return null;
      }
    }

    public void close() throws IOException {
      if (columnValues != null) {
        columnValues.clear();
        columnValues = null;
      }
    }
  }

  public Map<String, TabletMapFile[]> splitAndSaveCanCommit(Row.Key midRowKey,
      TabletInfo[] splitedTabletInfos) throws IOException {
    initMemoryForCompaction();
    
    try {
      return splitAndSave(midRowKey, splitedTabletInfos, compactingColumnCollections, false);
    } catch (Exception e) {
      cancelCompaction(null);
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        IOException err = new IOException(e.getMessage());
        err.initCause(e);
        throw err;
      }
    }
  }

  public void split(Row.Key midRowKey, Tablet[] splitedTablets) throws IOException {
    //LOG.debug(tabletInfo.getTabletName() + " compactingColumnCollections.size(): " +  compactingColumnCollections.size());
    for (Map.Entry<String, ColumnCollection> entry : columnCollections.entrySet()) {
      String columnName = entry.getKey();
      ColumnCollection columnCollection = entry.getValue();
      
      if (columnCollection.isEmpty()) {
        continue;
      }
      
      columnCollection.split(tabletInfo, midRowKey, splitedTablets, columnName, numOfVersion);
    }        
  }
  
  public Map<String, TabletMapFile[]> splitAndSave(Row.Key midRowKey,
      TabletInfo[] splitedTabletInfos) throws IOException {
    return splitAndSave(midRowKey, splitedTabletInfos, columnCollections, true);
  }

  private Map<String, TabletMapFile[]> splitAndSave(Row.Key midRowKey,
      TabletInfo[] splitedTabletInfos, Map<String, ColumnCollection> columnCollections,
      boolean doLock) throws IOException {
    long startTime = System.currentTimeMillis();
    Map<String, TabletMapFile[]> result = new HashMap<String, TabletMapFile[]>(10);

    String fileId = IdGenerator.getId();

    if (doLock) {
    }

    HashMap<String, ColumnCollection> clonedMap = null;
    columnCollectionsLock.readLock().lock();
    try {
      clonedMap = new HashMap<String, ColumnCollection>(columnCollections);
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
    
    try {
      for (Map.Entry<String, ColumnCollection> entry : clonedMap.entrySet()) {
        String columnName = entry.getKey();
        ColumnCollection columnCollection = entry.getValue();
        
        if (columnCollection.isEmpty()) {
          continue;
        }
        
        TabletMapFile[] mapFiles = columnCollection.splitAndSaveToDisk(tabletInfo, midRowKey,
            splitedTabletInfos, columnName, fileId, numOfVersion);
        result.put(columnName, mapFiles);
      }
      return result;
    } finally {
      if (doLock) {
      }
      LOG.debug("splitAndSave:" + tabletInfo.getTabletName() + ", time="
          + (System.currentTimeMillis() - startTime) + ",doLock=" + doLock);
    }
  }

  /**
   * @param compacting
   *          the compacting to set
   */
//  public void setCompacting(boolean compacting) {
//    this.compacting = compacting;
//  }

  public boolean isEmpty() {
    columnCollectionsLock.readLock().lock();
    try {
      return columnCollections.isEmpty() && compactingColumnCollections.isEmpty();
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
  }

  public String getTestHandlerKey() {
    return tabletServer.getHostName();
  }

  public TabletServer getTabletServer() {
    return tabletServer;
  }

  public String toString() {
    return "MemorySSTable:" + tabletInfo.getTabletName();
  }
}

