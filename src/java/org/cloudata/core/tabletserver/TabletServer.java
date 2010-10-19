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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.LeaseHolder;
import org.cloudata.core.common.LeaseListener;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.TestCaseTabletServerKillException;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.ipc.AclManager;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.ipc.CRPC.Server;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.Daemon;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.common.util.SizeOf;
import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.fs.PipeBasedCommitLogFileSystem;
import org.cloudata.core.master.CloudataMaster;
import org.cloudata.core.master.TabletMasterProtocol;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchemaMap;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;
import org.cloudata.core.tabletserver.action.ActionFinalizer;
import org.cloudata.core.tabletserver.action.BatchUploadAction;
import org.cloudata.core.tabletserver.action.TabletAction;
import org.cloudata.core.tabletserver.metrics.TabletServerMetrics;


/**
 * Tablet을 관리하는 메인 서버<br>
 * Cloudata을 구성하는 세가지 데몬(CloudataMaster, TabletServer, CommitLogServer) 서버 중 하나이다.<p> 
 * 다음과 같은 기능을 수행한다.<br>
 * <li>1. Tablet의 데이터 서비스(get, put)</li> 
 * 
 * @author babokim
 *
 */
public class TabletServer implements TabletManagerProtocol, DataServiceProtocol, Constants, Runnable, Watcher {
  //FIXME TabletDrop 처리 시 기존에 처리 중이던 작업에 대해서는 어떻게 하나???
  public static final Log LOG = LogFactory.getLog(TabletServer.class.getName());

  private static final int MAX_TABLET_LOAD_THREAD = 20;
  
  private int maxTabletCount = 400;

  protected CloudataConf conf;

  private String hostName;

  private Server server;

  private CloudataFileSystem fs;

  // tabletName, tablet
  private ConcurrentHashMap<String, Tablet> tablets = new ConcurrentHashMap<String, Tablet>();

  //private Set<TabletInfo> stopedTablets = Collections.synchronizedSet(new HashSet<TabletInfo>(100));
  
  // scannerId, tabletName
  private ConcurrentHashMap<String, String> tabletScanners = new ConcurrentHashMap<String, String>(10);

  private boolean stopRequested = false;

  private TabletMasterProtocol masterServer;
  
  protected LeaseHolder leaseHolder;

  // tableName, Table
  protected TableSchemaMap schemaMap;

  protected boolean daemonMode = false;   //TestCase를 위해 설정
  
  //Scan, Bulk upload 등과 같이 많은 데이터 처리시 Stream 처리를 위해 사용
  private Daemon dataXceiveServer;
  
  private HeartbeatThread heartbeatThread;
  
  private MinorCompactionCheckThread minorCompactionThread;
  
  private MajorCompactionSplitCheckThread majorCompactionThread;
  
  private Count scanCount = new Count(0);
  
  private Count uploaderCount = new Count(0);
  
  private String tabletServerLockPath;

  protected ThreadGroup threadGroup;
  
  private boolean testMode;
  
  private ExecutorService loadThreadExecutor = Executors.newFixedThreadPool(MAX_TABLET_LOAD_THREAD);
  
  protected ThreadPoolExecutor compactionExecutor;
  
  protected ThreadPoolExecutor splitExecutor;
  
  protected ThreadPoolExecutor actionExecutor;

  protected  AsyncTaskManager asyncTaskManager = new AsyncTaskManager(); 
  
  //0: path, 1:lockKey
  //protected LinkedList <String[]> acquiredScLockPath = new LinkedList <String[]>();

  //0: path, 1:lockKey
  protected LinkedHashSet <String[]> acquiredMinorCompactionLockPath = new LinkedHashSet<String[]>();

  protected Date serverStartTime;
  
  //TabletName -> BatchUploadAction
  private Map<String, Map<String, BatchUploadAction>> batchUploadActions = 
    Collections.synchronizedMap(new HashMap<String, Map<String, BatchUploadAction>>(10));
  
  //TabletServer에 메모리 부하가 발생한 경우 apply 처리 후 delay 시킨다.
  private AtomicInteger heavyMemory = new AtomicInteger(0);

  //private AtomicBoolean heavyThread = new AtomicBoolean(false);
  
  public TabletServerMetrics tabletServerMetrics;
  
  private ZooKeeper zk;
  
  private int maxMajorCompactionThread;
  
  private int maxSplitThread;
  
  private Random applyRandom = new Random();
  
  private AtomicLong lastMinorCompactionSpeed = new AtomicLong(0);
  private AtomicLong lastMinorCompactionTime = new AtomicLong(0);
  private ReentrantLock lastMinorCompactionSpeedLock = new ReentrantLock();
  private long minSpeed = 0;
  private AtomicBoolean intensivePut = new AtomicBoolean(false); 
  private AtomicBoolean tabletDistributionMode = new AtomicBoolean(true);
  
  private int maxResultRecord;

  //private Set<String> liveCommitLogServers = new HashSet<String>();

  protected CommitLogErrorMonitor commitLogErrorMonitor;
  
  protected long maxMemoryCacheCapacity;
  
  protected AtomicLong currentMemoryCacheSize = new AtomicLong(0);
  
  public TabletServer()  {
  }
  
  public void init(CloudataConf conf) throws IOException {
    this.serverStartTime = new Date();
    this.testMode = conf.getBoolean("testmode", false);
    this.conf = conf;
    
    this.maxMajorCompactionThread = this.conf.getInt("tabletServer.maxMajorCompactionThread", 5);
    this.maxSplitThread = this.conf.getInt("tabletServer.maxSplitThread", 5);
    
    this.compactionExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(maxMajorCompactionThread);
    this.splitExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(maxSplitThread);
    this.actionExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(this.conf.getInt("tabletServer.maxMinorCompactionThread", 10));
    
    this.maxTabletCount = conf.getInt("tabletServer.max.tablet.count", 2000);

    this.maxResultRecord = conf.getInt("client.max.resultRecord", 5000);

    this.maxMemoryCacheCapacity = conf.getLong("memory.maxColumnCacheCapacity", 200) * 1024 * 1024;
    
    this.fs = CloudataFileSystem.get(conf);

    if(fs == null || !fs.isReady()) {
      LOG.fatal("FileSystem is not ready. TabletServer shutdown");
      shutdown();
    }
    
    InetSocketAddress serverAddress = NetworkUtil.getAddress(InetAddress.getLocalHost().getHostName() + ":"
        + conf.getInt("tabletServer.port", 7001));
    
    this.hostName = serverAddress.getHostName() + ":" + serverAddress.getPort();
    
    this.threadGroup = new ThreadGroup("TabletServer_" + hostName);
    
    this.leaseHolder = new LeaseHolder(threadGroup);
    
    this.tabletServerLockPath = Constants.SERVER + "/" + hostName;
    
    this.zk = LockUtil.getZooKeeper(conf, hostName, this);
    
    //<Split Lock 삭제>
    try {
      LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.TABLETSERVER_SPLIT + "/" + hostName), true);
    } catch (Exception e) {
      throw new IOException(e);
    }
    //</Split Lock 삭제>
    
    schemaMap = new TableSchemaMap(conf, zk);

    tabletServerMetrics = new TabletServerMetrics(conf, this);

    this.server = CRPC.getServer(zk, this, serverAddress.getHostName(), serverAddress.getPort(), 
            conf.getInt("tabletServer.handler.count", 10), false, conf, tabletServerMetrics);
    
    ServerSocket ss = null;
    int port = conf.getInt("tabletServer.scanner.port", 50100);
    String bindAddress = "0.0.0.0";
    
    try {
//      LOG.info("Opened Scanner Handler at " + hostName  + ", port=" + port);
      ss = new ServerSocket(port,0,InetAddress.getByName(bindAddress));
      ss.setReuseAddress(true);
    } catch (IOException ie) {
      LOG.error("Could not open scanner server at " + port + ", stop server and Stop tablet server", ie);
      exit();
    }
    this.dataXceiveServer = new Daemon(new DataXceiveServer(ss));  
    
    try {
      LockUtil.createNodes(zk, LockUtil.getZKPath(conf, tabletServerLockPath), hostName.getBytes(), CreateMode.EPHEMERAL);
      LOG.info("TableServer lock created:" + LockUtil.getZKPath(conf, tabletServerLockPath));
    } catch (Exception e) {
      LOG.fatal("TabletServer stopped. Can't server lock:" + tabletServerLockPath, e);
      exit();
    }
    
    if (tabletDistributionMode.get()) {
      LOG.info("Turn on tablet distribution mode");
    }

    heartbeatThread = new HeartbeatThread();
    heartbeatThread.setDaemon(true);
    heartbeatThread.start();
  }


  public void run() {
    try {
      daemonMode = true;

      this.server.start();

      minorCompactionThread = new MinorCompactionCheckThread();
      minorCompactionThread.setDaemon(true);
      minorCompactionThread.start();
      
      majorCompactionThread = new MajorCompactionSplitCheckThread();
      majorCompactionThread.setDaemon(true);
      majorCompactionThread.start();
      
      dataXceiveServer.start();
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error(e.getMessage(), e);
      exit();
    }

    LOG.info("TabletServer[" + hostName + "] started");
    try {
      this.server.join();
    } catch (InterruptedException ie) {
      return;
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      switch (event.getState()) {
      case SyncConnected:
        break;
      case Disconnected:
	LOG.warn("Disconnected:" + event);
	break;
      case Expired:
        LOG.fatal("Shutdown cause lock expired:" + event);
        shutdown();
        break;
      }
    }
  }
  
  public String getTabletServerConf(String key) throws IOException {
    return conf.get(key);
  }
  
  public String getHostName() {
    return this.hostName;
  }
  
  public void printTabletInfo(String tabletName) throws IOException {
    Tablet tablet = getTablet(tabletName);
    System.out.println("=====================================");
    System.out.println("          " + tabletName);
    System.out.println("-------------------------------------");
    if (tablet == null) {
      System.out.println("No tablet");
    } else {
      tablet.print();
    }
    System.out.println("=====================================");
  }

  public String[] getAllActions(String tabletName) throws IOException {
    Tablet tablet = getTablet(tabletName);

    if (tablet == null) {
      throw new IOException("no tablet[" + tabletName + "]");
    }
    
    return tablet.getAllActions();
  }
  
  public void assignTablet(TabletInfo tabletInfo) throws IOException {
    LOG.debug("assigned tablet: on " + hostName + ", tablet="+ tabletInfo);
    try {
      if(tablets.size() >= maxTabletCount) {
        LOG.warn("Can't load table[" +  tabletInfo.getTabletName() + "] cause by tablets.size()[" + 
            tablets.size() + "] exceeds MAX_TABLET_COUNT[" + maxTabletCount + "]");
        connectMasterServer().errorTabletAssignment(hostName, tabletInfo);
      }
    } catch (IOException e) {
      LOG.error("Can't load table[" +  tabletInfo.getTabletName() + "]", e);
      return;
    }
    
    loadThreadExecutor.execute(new TabletLoadTask(tabletInfo));
  }
  
  public int getTabletCount() {
    return tablets.size();
  }

  public TabletInfo[] reportTablets() {
    ArrayList<TabletInfo> tabletInfoList = new ArrayList<TabletInfo>();
    Enumeration<Tablet> en = tablets.elements();
    while(en.hasMoreElements()) {
      Tablet eachTablet = en.nextElement();
      tabletInfoList.add(eachTablet.getTabletInfo());
    }
    return tabletInfoList.toArray(new TabletInfo[0]);
  }

  private void fillTimestamp(CommitLog[] commitLogList) {
    // by sangchul
    // timestamp로써 역할을 할 수 있을까? 값이 다 똑같진 않을까?
    for(CommitLog log : commitLogList) {
      if (log.timestamp == CommitLog.USE_SERVER_TIMESTAMP) {
        log.timestamp = System.currentTimeMillis();
      }
    }
  }

  public long apply(String tabletName, Row.Key rowKey, TxId txId, CommitLog[] commitLogList, boolean saveLog) throws IOException {
    if(tabletName == null) {
      throw new IOException("tabletName parameter is null");
    }
    
    if(rowKey == null) {
      throw new IOException("rowKey parameter is null");
    }
    
    if(txId == null) {
      throw new IOException("txId parameter is null");
    }
    
    if(commitLogList == null || commitLogList.length == 0) {
      throw new IOException("commitLogList parameter is null");
    }
    
    fillTimestamp(commitLogList);
    long startTime = System.nanoTime();
    
    Tablet tablet = getTabletByName(tabletName);
    
    if(heavyMemory.get() == 2) {
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_PUT_SKIP, (System.nanoTime() - startTime));
      return conf.getLong("tabletServer.heavyMemoryPutSleep", 200) * 2;
    }
    
    //Heavy memory status(%)
    if( (heavyMemory.get() == 1 || 
        tablet.getMemtableSize() > 200 * 1024 * 1024 ) && 
        !electApplyAccept(conf.getFloat("tabletServer.put.heavyMemoryAcceptRate", 0.3f))) {
      if(!isMeta(tablet)) {
        tabletServerMetrics.addTx(TabletServerMetrics.TYPE_PUT_SKIP, (System.nanoTime() - startTime));
        return conf.getLong("tabletServer.heavyMemoryPutSleep", 200);
      }
    }
    
    //data file size check
    if(tablet.getDiskSSTable().sumMapFileSize() > tablet.maxTabletSizeByte * 2.0f &&
       !electApplyAccept(conf.getFloat("tabletServer.put.heavyMemoryAcceptRate", 0.5f))) {
      if(!isMeta(tablet)) {
        tabletServerMetrics.addTx(TabletServerMetrics.TYPE_PUT_SKIP, (System.nanoTime() - startTime));
        return 100;
      }
    }
    
    //tablet split status(%)
    if(tablet.isSplitting() && !electApplyAccept(conf.getFloat("tabletServer.put.splitingAcceptRate", 0.4f))) {    
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_PUT_SKIP, (System.nanoTime() - startTime));
      return conf.getLong("tabletServer.tabletSplitPutSleep", 100);
    }
    
    boolean success = tablet.apply(rowKey, txId, commitLogList, saveLog);
    
    if(success) {
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_PUT, (System.nanoTime() - startTime));
    } else {
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_PUT_FAIL, (System.nanoTime() - startTime));
    }

    return success ? 0 : conf.getInt("tx.retry.sleeptime.msec", 100);
  }
  
  private boolean isMeta(Tablet tablet) {
    return Constants.TABLE_NAME_META.equals(tablet.getTabletInfo().getTableName()) || 
        Constants.TABLE_NAME_ROOT.equals(tablet.getTabletInfo().getTableName());
  }
  
  private boolean electApplyAccept(float acceptRate) {
    int min = (int)(10.0 * acceptRate);
    
    int randNum = applyRandom.nextInt(10);
    return randNum < min;
  }

  public boolean startFlush(String tabletName, TxId txId) throws IOException {
    boolean success = getTabletByName(tabletName).startFlush(txId);

    if(success) {
      leaseHolder.createLease(txId, new FlushListener(tabletName, txId), 30 * 1000);
    }
    
    return success;
  }
  
  public void endFlush(String tabletName, TxId txId) throws IOException {
    try {
      getTabletByName(tabletName).endFlush(txId);
      leaseHolder.removeLease(txId);
    } catch (IOException e) {
      return;
    }
  }
  
  class FlushListener implements LeaseListener {
    String tabletName;

    TxId txId;

    public FlushListener(String tabletName, TxId txId) {
      this.tabletName = tabletName;
      this.txId = txId;
    }

    public void expire() {
      try {
        getTabletByName(tabletName).endFlush(txId);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  } // end of TxListener class
  
  private Tablet getTabletByName(String tabletName) throws IOException {
    Tablet tablet = tablets.get(tabletName);

    if(tablet == null) {
      throw new IOException("no tablet[" + tabletName + "] in tablet server@" 
          + InetAddress.getLocalHost().getHostName());
    }
    
    return tablet;
  }

  public ColumnValue get(String tabletName, Row.Key rowKey, 
      String columnName, Cell.Key columnKey) throws IOException {
    Tablet tablet = getTablet(tabletName);

    if (tablet == null) {
      throw new IOException("no tablet[" + tabletName + "]");
    }

    //NStopWatch watch = new NStopWatch();
    //watch.start("TabletServer.get0, AclManager.checkPermission", 10);
    AclManager.checkPermission(
        conf,
        zk,
        schemaMap, tablet.getTabletInfo().getTableName(), "r");
    //watch.stopAndReportIfExceed(LOG);
    
    long startTime = System.nanoTime();
    try {
      return tablet.get(rowKey, columnName, columnKey);
    } finally {
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_GET, (System.nanoTime() - startTime));
    }
  }

  public RowColumnValues[][] gets(String tabletName, Row.Key startRowKey,
      RowFilter rowFilter) throws IOException {
    Tablet tablet = getTablet(tabletName);
    
    if (tablet == null) {
      throw new IOException("no tablet[" + tabletName + "]");
    }
    
    rowFilter.setStartRowKey(startRowKey);
    String[] columnNames = rowFilter.getColumns();
    
    long startTime = System.nanoTime();
    
    TabletScanner[] scanners = new TabletScanner[columnNames.length];
    ServerSideMultiColumnScanner multiColumnScanner = null;
    try {     
      for(int i = 0; i < columnNames.length; i++) {
        scanners[i] = tablet.openScanner(columnNames[i], rowFilter, false);
        if(scanners[i]  == null) {
          throw new IOException("Can't open scanner:" + tabletName);
        }
        String scannerId = scanners[i].getScannerId();
        tabletScanners.put(scannerId, tabletName);
        int scannerExpireTime = conf.getInt("client.tx.timeout", 30) * 2 * 1000;
        leaseHolder.createLease(scannerId, new TabletScannerListener(tabletName, scannerId), scannerExpireTime);
      }
      
      List<RowColumnValues[]> result = new ArrayList<RowColumnValues[]>();
      
      multiColumnScanner = new ServerSideMultiColumnScanner(this, rowFilter, scanners);
  
      RowColumnValues[] rowColumnValue = null;
      int rowCount = 0;
      int maxRowNum = rowFilter.isPaging() ? rowFilter.getPageSize() : maxResultRecord;
      
      while((rowColumnValue = multiColumnScanner.nextRow()) != null) {
        result.add(rowColumnValue);
        rowCount++;
        if(rowCount >= maxRowNum) {
          break;
        }
      }
      return result.toArray(new RowColumnValues[][]{});
    } finally {
      if(multiColumnScanner != null) {
        multiColumnScanner.close(false);
      } else {
        for(int i = 0; i < scanners.length; i++) {
          if(scanners[i] != null) {
            try {
              closeScanner(scanners[i].getScannerId(), false);
            } catch (Exception e) {
              LOG.error("Can't close scanner:" + e.getMessage(), e);
            }
          }
        }
      }
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_GET, (System.nanoTime() - startTime));
    }
  }
  
  public RowColumnValues[] get(String tabletName, RowFilter rowFilter) throws IOException {
    Tablet tablet = getTablet(tabletName);
  
    if (tablet == null) {
      throw new IOException("no tablet[" + tabletName + "]");
    }

    AclManager.checkPermission(
        conf,
        zk,
        schemaMap, tablet.getTabletInfo().getTableName(), "r");
    
    long startTime = System.nanoTime();
    try {
      RowColumnValues[] row = tablet.get(rowFilter);
      return row;
    } finally {
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_GET, (System.nanoTime() - startTime));
    }
  }
  
  public ColumnValue[][] get(String tabletName, Row.Key rowKey) throws IOException {
    Tablet tablet = getTablet(tabletName);
    
    if (tablet == null || !tablet.isReady()) {
      throw new IOException("no tablet[" + tabletName + "] on " + hostName);
    }
    
    AclManager.checkPermission(
        conf,
        zk,
        schemaMap, tablet.getTabletInfo().getTableName(), "r");
    
    List<String> columnList = descTable(tabletName).getColumns();
    
    long startTime = System.nanoTime();
    try {
      return tablet.get(rowKey, columnList.toArray(new String[columnList.size()]));
    } finally {
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_GET, (System.nanoTime() - startTime));
    }
  }


  public ColumnValue[][] get(String tabletName, Row.Key rowKey, String[] columnNames) throws IOException {
    Tablet tablet = getTablet(tabletName);
    
    if (tablet == null || !tablet.isReady()) {
      throw new IOException("no tablet[" + tabletName + "] on " + hostName);
    }
    
    AclManager.checkPermission(
        conf,
        zk,
        schemaMap, tablet.getTabletInfo().getTableName(), "r");

    long startTime = System.nanoTime();
    try {   
      return tablet.get(rowKey, columnNames);
    } finally {
      tabletServerMetrics.addTx(TabletServerMetrics.TYPE_GET, (System.nanoTime() - startTime));
    }
  }
  
  public Tablet getTablet(String tabletName) {
    return tablets.get(tabletName);
  }

  private TableSchema descTable(String tableName) throws IOException {
    return schemaMap.get(tableName);
  }

  private ColumnValue[] next(String scannerId) throws IOException {
    String tabletName = tabletScanners.get(scannerId);

    if (tabletName == null) {
      throw new IOException("[" + scannerId + "] not opened");
    }
    
    Tablet tablet = getTablet(tabletName);
    
    if (tablet == null) {
     throw new IOException("[" + tabletName + "] not serviced on this server(" + hostName + ")");
    }
    
    leaseHolder.touch(scannerId);

    return tablet.scanNext(scannerId);
  }
  
  public void touch(String leaseId) throws IOException {
    leaseHolder.touch(leaseId);
  }

  /**
   * 
   */
  private String openScanner(String tabletName, String columnName, RowFilter scanFilter)
      throws IOException {
    Tablet tablet = getTablet(tabletName);
    if (tablet == null) {
      throw new IOException("[" + tabletName + "] not serviced on this server [" + hostName + "]");
    }

    // FIXME Scanner가 오픈되어있을때 Minor, Major Compaction이 발생하면 어떻게 하는가?
    TabletScanner tabletScanner = tablet.openScanner(columnName, scanFilter);
    if(tabletScanner == null) {
      return null;
    }
    String scannerId = tabletScanner.getScannerId();

    tabletScanners.put(scannerId, tabletName);

    int scannerExpireTime = conf.getInt("client.tx.timeout", 60) * 2 * 1000;
    leaseHolder.createLease(scannerId, new TabletScannerListener(tabletName, scannerId), scannerExpireTime);

    tabletServerMetrics.numScannerOpen.incrementAndGet();

    return scannerId;
  }

  
  protected void closeScanner(String scannerId) throws IOException {
    closeScanner(scannerId, true);
  }
  
  protected void closeScanner(String scannerId, boolean checkAction) throws IOException {
    String tabletName = null;
    tabletName = tabletScanners.get(scannerId);

    if (tabletName == null) {
      throw new IOException("[" + scannerId + "] not opened");
    }
    
    Tablet tablet = getTablet(tabletName);
    if (tablet != null) {
      tablet.closeScanner(scannerId, checkAction);
    }
    
    tabletScanners.remove(scannerId);

    leaseHolder.removeLease(scannerId);
  }

  public boolean dropTable(final String taskId, final String tableName) throws IOException {
    //META에서 Tablet을 삭제하고 TabletServer의 변수에서 삭제한다.
    //데이터 파일에 대한 삭제는 CloudataMaster에서 일괄 삭제한다.
    LOG.debug("Drop table:" + hostName + ": " + tableName);
    
    final List<Tablet> dropTablets = new ArrayList<Tablet>();
    Enumeration<Tablet> e = tablets.elements();
    while(e.hasMoreElements()) {
      Tablet tablet = e.nextElement();
      if(tableName.equals(tablet.getTabletInfo().getTableName())) {
        dropTablets.add(tablet);
      }
    }
     
    schemaMap.remove(tableName);
    
    if(dropTablets.isEmpty()) {
      return false;
    } 
    
    Thread dropThread = new Thread(threadGroup, new Runnable() {
      public void run() {
        try {
          CTable ctable = CTable.openTable(conf, TABLE_NAME_META);
          for(Tablet eachTablet: dropTablets) {
            TabletInfo tabletInfo = eachTablet.getTabletInfo();
            
            tablets.remove(tabletInfo.getTabletName());

            //META에서 Tablet 정보를 삭제한다.
            LOG.debug("Drop table:delete from META:" + tabletInfo);
            Row.Key rowKey = Tablet.generateMetaRowKey(tabletInfo.getTableName(), tabletInfo.getEndRowKey());
            ctable.remove(rowKey, Constants.META_COLUMN_NAME_TABLETINFO, new Cell.Key(tabletInfo.getTabletName()));

            eachTablet.drop();
            
            eachTablet = null;
          }
          connectMasterServer().endTableDrop(taskId, hostName, tableName);
        } catch (Exception e) {
          LOG.error("Drop error:" + tableName, e);
          try {
            connectMasterServer().errorTableDrop(taskId, hostName, tableName, e.getMessage());
          } catch (IOException e1) {
            LOG.fatal("Error while report error table drop:" + tableName, e1);
          }
        } 
      }
    });

    dropThread.start();
    
    return true;
  }
  
  /**
   * ROOT, META tablet에 tablet에 대한 정보를 등록한다.
   * 
   * @param tabletInfo
   * @throws IOException
   */
  private void insertTableInfo(TabletInfo tabletInfo, long timestamp) throws IOException {   
    Exception err = null;
    int retry = 0;
    CTable ctable = null;
    while(retry < 50) {
      try {
        if (TABLE_NAME_META.equals(tabletInfo.getTableName())) {
          ctable = CTable.openTable(conf, TABLE_NAME_ROOT);
        } else {
          ctable = CTable.openTable(conf, TABLE_NAME_META);
        }
        Row row = new Row(Tablet.generateMetaRowKey(tabletInfo.getTableName(), tabletInfo.getEndRowKey()));
        row.addCell(META_COLUMN_NAME_TABLETINFO, new Cell(new Cell.Key(tabletInfo.getTabletName()), tabletInfo.getWriteBytes(), timestamp));
        ctable.put(row);
        LOG.debug("Success insertTableInfo:" + tabletInfo);
        return;
      } catch (Exception e) {
        LOG.error("fail insertTableInfo " + ctable + " but retry.", e);
        err = e;
        retry++;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          return;
        }        
      }
    }
    if(retry >= 50) {
      if(err != null) {
        LOG.error("fail insertTableInfo after 50 times retry. last error is:", err);
        throw new IOException("fail insertTableInfo " + ctable + " after 50 times retry", err);
      } else {
        LOG.error("fail insertTableInfo " + ctable + " after 50 times retry");
        throw new IOException("fail insertTableInfo " + ctable + " after 50 times retry");
      }
    } 
  }

  /**
   * 
   * @param tabletInfo
   * @throws IOException
   */
  protected void modifyTableInfo(TabletInfo tabletInfo) throws IOException {
    Exception err = null;
    int retry = 0;
    CTable ctable = null;
    while(retry < 50) {
      try {
        if (TABLE_NAME_META.equals(tabletInfo.getTableName())) {
          ctable = CTable.openTable(conf, TABLE_NAME_ROOT);
        } else {
          ctable = CTable.openTable(conf, TABLE_NAME_META);
        }
        Row.Key rowKey = Tablet.generateMetaRowKey(tabletInfo.getTableName(), tabletInfo.getEndRowKey());
        
        Row row = new Row(rowKey);
        
        row.addCell(META_COLUMN_NAME_TABLETINFO, new Cell(new Cell.Key(tabletInfo.getTabletName()), tabletInfo.getWriteBytes()));
        ctable.put(row);

        LOG.debug("Success modifyTableInfo:" + tabletInfo);
        
        return;
      } catch (Exception e) {
        LOG.error("fail modifyTableInfo " + ctable + " but retry.", e);
        err = e;
        retry++;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
          return;
        }        
      }
    }
    if(retry > 0) {
      if(err != null) {
        LOG.error("fail modifyTableInfo after 50 times retry. last error is:", err);
        throw new IOException("fail modifyTableInfo " + ctable + " after 50 times retry", err);
      } else {
        LOG.error("fail modifyTableInfo " + ctable + " after 50 times retry");
        throw new IOException("fail modifyTableInfo " + ctable + " after 50 times retry");
      }
    }    
  }
  
  /**
   * 
   * @param targetTablet
   * @param splitedTablets
   * @throws IOException
   */
  public void tabletSplited(Tablet targetTablet, Tablet[] splitedTablets) throws IOException {
    if(!tablets.containsKey(targetTablet.getTabletInfo().getTabletName()) ||
        targetTablet.isDroped()) {
      //Drop 처리된 경우 split은 무효화 된다.
      throw new IOException("Can't split cause droped tablet:" + targetTablet.getTabletInfo().getTabletName());
    }
    
    //////////////////////////////////////////////////////////////////
    //META 수정 후 TabletServer가 kill 되는 상황에 대비해서 split 된 정보 생성
    String splitLockPath = getSplitLockPath(targetTablet);
    
    //tableName + maxRow.Key, maxRow.Key
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(out);
    CWritableUtils.writeString(dout, targetTablet.getTabletInfo().getTableName());
    splitedTablets[0].getTabletInfo().write(dout);
    splitedTablets[1].getTabletInfo().write(dout);
    
    LockUtil.createNodes(zk, LockUtil.getZKPath(conf, splitLockPath), 
        out.toByteArray(), CreateMode.PERSISTENT); 
    //////////////////////////////////////////////////////////////////
    
    //Split 되었지만 finish 작업 도중 오류가 발생한 경우 META 데이터를 
    //이용하여 불필요한 tablet에 대한 내용을  파일시스템에서 삭제하는 로직이 필요. --> CloudataMaster에서 수행 
    modifyMetaDataForSplit(targetTablet, splitedTablets);

    try {
      //두번째 Tablet은 자신이 서비스하고 첫번째 Tablet은 다른 TabletServer로 할당시킨다.
      tablets.remove(targetTablet.getTabletInfo().getTabletName());
      tablets.put(splitedTablets[1].getTabletInfo().getTabletName(), splitedTablets[1]);
    } catch (Exception e) {
      //장애 발생시 자동 종료(장애 발생 확율은 0% 이지만 그래도 혹시나 발생시는 자신을 스스로 kill
      LOG.fatal("TableServer shuwdowned cause fail to remove/put after split", e);
      shutdown();
    }

    //NStopWatch watch = new NStopWatch();
    //watch.start("tabletSplited, reportTabletSplited", 50);
    reportTabletSplited(targetTablet, splitedTablets);
    //watch.stopAndReportIfExceed(LOG);
    
    //watch.start("lockService deleteNode", 50);
    try {
      LockUtil.delete(zk, LockUtil.getZKPath(conf, splitLockPath));
      LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.UPLOADER + "/" + targetTablet.getTabletInfo().getTabletName()), true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private String getSplitLockPath(Tablet targetTablet) {
    return Constants.TABLETSERVER_SPLIT + "/" + hostName + "/" + targetTablet.getTabletInfo().getTabletName();
  }
  
  public void reportTabletSplited(Tablet targetTablet, Tablet[] splitedTablets) throws IOException {
    try {
      TabletInfo[] splitedTabletInfos = new TabletInfo[] { splitedTablets[0].getTabletInfo(),
          splitedTablets[1].getTabletInfo() };
  
      //여기서 장애가 발생하는 경우는 CloudataMaster가 정상적인 상황인 아닌 경우
      //따라서 CloudataMaster가 재 시작되면 Split된 Tablet은 다시 다른 TabletServer로 할당됨
      //TODO drop 처리 중인 상황에 대한 동기화 처리
      connectMasterServer().reportTabletSplited(targetTablet.getTabletInfo(), splitedTabletInfos);
    } catch (TestCaseTabletServerKillException e) {
      throw e;
    } catch (Exception e) {
      LOG.info("Tablet creation report fail", e);
    }
  }

  public void modifyMetaDataForSplit(Tablet targetTablet, Tablet[] splitedTablets) throws IOException {
    if(targetTablet.isDroped()) {
      return;
    }
    /*
     * META의 경우 하나의 Table에 대한 정보는 하나의 META tablet에서 관리하도록 split한다.
     * (이 부분은 아직 구현되지 않았음)
     * 따라서 여기서 하나의 tablet이 split 되었을 경우 발생하는 세개의 tx(delete old, insert new1, insert new2)를
     * Tablet에서는 하나의 tx 처리하는 것과 동일하게 처리한다.
     */    
    int maxRetry = 100;
    try {
      //처리 도중 kill 되는 경우에 META에는 변경된 내용이 반영 안됨
      if (TABLE_NAME_META.equals(targetTablet.getTabletInfo().getTableName())) {
        //META table인 경우
        int retry = 0;
        while(retry < maxRetry) {
          try {
            TabletInfo rootTabletInfo = Tablet.getRootTabletInfo(conf, zk);
            DataServiceProtocol rootTabletServer = (DataServiceProtocol) CRPC.getProxy(DataServiceProtocol.class,
                DataServiceProtocol.versionID, NetworkUtil.getAddress(rootTabletInfo.getAssignedHostName()), conf);    
            
            rootTabletServer.saveTabletSplitedInfo(
                                    rootTabletInfo.getTabletName(), 
                                    targetTablet.getTabletInfo(), 
                                    new TabletInfo[]{splitedTablets[0].getTabletInfo(), 
                                      splitedTablets[1].getTabletInfo()});
            return;
          } catch (Exception e) {
            retry++;
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e1) {
                return;
            }
          }
        }
        if(retry >= maxRetry) {
          throw new IOException("fail saveTabletSplitedInfo to ROOT");
        }
      } else {
        //일반 table인 경우
        Row.Key rowKey = Tablet.generateMetaRowKey(targetTablet.getTabletInfo().getTableName(), targetTablet.getTabletInfo().getEndRowKey());
        StringBuffer metaTabletName = new StringBuffer(10);
        int retry = 0;
        Exception err = null;
        while(retry < maxRetry) {
          try {
            metaTabletName = new StringBuffer(10);
            DataServiceProtocol metaTabletServer = this.connectMetaTabletServer(targetTablet.getTabletInfo(), metaTabletName, rowKey);
            metaTabletServer.saveTabletSplitedInfo(metaTabletName.toString(), targetTablet.getTabletInfo(), 
                                                                new TabletInfo[]{splitedTablets[0].getTabletInfo(), splitedTablets[1].getTabletInfo()});
            LOG.debug("Success modifyMetaDataForSplit: " + targetTablet.getTabletInfo());
            return;
          } catch (Exception e) {
            LOG.warn(targetTablet.getTabletInfo() + ": " + e.getMessage());
            retry++;
            err = e;
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
              return;
            }
          }
        }
        if(retry >= maxRetry) {
          throw new IOException("fail saveTabletSplitedInfo to " + metaTabletName + "," + err, err);
        }
      }
    } catch (Exception e) {
      LOG.error(e);
      throw new IOException(e);
    }
  }

  private DataServiceProtocol connectMetaTabletServer(TabletInfo tabletInfo, StringBuffer metaTabletNameBuffer,
      Row.Key rowKey) throws IOException {
    TabletInfo rootTabletInfo = Tablet.getRootTabletInfo(conf, zk);
    DataServiceProtocol rootTabletServer = (DataServiceProtocol) CRPC.getProxy(DataServiceProtocol.class,
    DataServiceProtocol.versionID, NetworkUtil.getAddress(rootTabletInfo.getAssignedHostName()), conf);
    
    // find meta tablet from root tablet
    Row.Key rootTabletSearchRowKey = Tablet.generateMetaRowKey(TABLE_NAME_META, rowKey);
    ColumnValue metaTabletRecord = rootTabletServer.getCloestMetaData(rootTabletInfo.getTabletName(), 
    rootTabletSearchRowKey);
    if(metaTabletRecord == null) {
    //LOG.error("meta tablet info is null[" + tabletInfo + ", rowKey=" + rowKey + "]");
    return null;
    }
    TabletInfo metaTablet = new TabletInfo();
    metaTablet.readFields(metaTabletRecord.getValue());
    
    String metatabletName = metaTablet.getTabletName();
    String assignedHost = metaTablet.getAssignedHostName();
    
    if (assignedHost == null) {
      LOG.error("meta tablet [" + metatabletName + "] not assigned");
      return null;
    }
    DataServiceProtocol metaTabletServer = (DataServiceProtocol) CRPC.getProxy(DataServiceProtocol.class,
    DataServiceProtocol.versionID, NetworkUtil.getAddress(assignedHost), conf);
    
    metaTabletNameBuffer.append(metatabletName);
    return metaTabletServer;
  }
  
  public void saveTabletSplitedInfo(String tabletName, TabletInfo targetTablet, TabletInfo[] splitedTablets) throws IOException {
    final Tablet tablet = getTablet(tabletName);
    if (tablet == null) {
      LOG.error("saveTabletSplitedInfo: [" + tabletName + "] not serviced on this server(" + hostName + ")");
      throw new IOException("[" + tabletName + "] not serviced on this server(" + hostName + ")");
    }
    
    String tableName = tablet.getTabletInfo().getTableName();
    if(!TABLE_NAME_ROOT.equals(tableName) && !TABLE_NAME_META.equals(tableName)) {
      LOG.error("saveTabletSplitedInfo: [" + tableName + "] is not META or ROOT");
      throw new IOException(" [" + tableName + "] is not META or ROOT");
    }
    
    Row.Key rowKey = Tablet.generateMetaRowKey(targetTablet.getTableName(), targetTablet.getEndRowKey());
    
    TxId txId = TxId.generate(tabletName, true);
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(bout);
    targetTablet.write(out);
    for(int i = 0; i < splitedTablets.length; i++) {
      splitedTablets[i].write(out);
    }
    CommitLog commitLog = new CommitLog(Constants.LOG_OP_MODIFY_META, 
                        tabletName, 
                        rowKey,
                        META_COLUMN_NAME_TABLETINFO,
                        new Cell.Key(""),
                        System.currentTimeMillis(),
                        bout.toByteArray());     
    
    if(!tablet.apply(rowKey, txId, new CommitLog[]{commitLog}, true)) {
      LOG.debug("Can't saveTabletSplitedInfo cause tablet is busy:" + tabletName);
      throw new IOException(tabletName + " tablet is busy");
    }
  }
  
  public void stopAllTablets() throws IOException {
    tablets.clear();
  }
  
  public static void main(String args[]) throws IOException {
//    try {
//      Thread.sleep(5 * 1000);
//    } catch (InterruptedException e) {
//      return;
//    }    
    try {
      StringUtils.startupShutdownMessage(TabletServer.class, args, LOG);
      TabletServer tabletServer = new TabletServer();
      tabletServer.init(new CloudataConf());
      (new Thread(tabletServer)).start();
    } catch (Exception e) {
      LOG.fatal("Error while start TabletServer:" + e.getMessage(), e);
      System.exit(0);
    }
  }

  private TabletMasterProtocol connectMasterServer() throws IOException {
    return connectMasterServer(3);
  }
  
  private TabletMasterProtocol connectMasterServer(int maxRetry) throws IOException {
    int retry = 0;
    while (retry < maxRetry) {
      try {
        if (masterServer == null) {
          String masterHostName = CloudataMaster.getMasterServerHostName(conf, zk);
          masterServer = (TabletMasterProtocol) CRPC.getProxy(TabletMasterProtocol.class,
                TabletMasterProtocol.versionID, NetworkUtil.getAddress(masterHostName), conf);
        }
        masterServer.checkServer();
        break;
      } catch (Exception e) {
        masterServer = null;
        retry++;
        if (retry >= maxRetry) {
          break;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          return null;
        }
      }
    }
    return masterServer;
  }
  
  public TabletInfo getTabletInfo(String tabletName) {
    Tablet tablet = getTablet(tabletName);
    if (tablet == null)
      return null;

    return tablet.getTabletInfo();
  }

  public boolean isServicedTablet(String tabletName) {
    return tablets.containsKey(tabletName);
  }

  public ColumnValue getCloestMetaData(String tabletName, Row.Key rowKey) throws IOException {
    //long startTime = System.currentTimeMillis();
    Tablet tablet = getTablet(tabletName);
    if (tablet == null) {
      LOG.debug("getCloest:[" + tabletName + "] not serviced on this server(" + hostName + ")");
      return null;
    }

    ColumnValue columnValue = tablet.getClosestMeta(rowKey);
    //System.out.println(Thread.currentThread().getName() + ".getCloestMetaData():" + (System.currentTimeMillis() - startTime));
    return columnValue;
  }

  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    if (protocol.equals(DataServiceProtocol.class.getName())) {
      return DataServiceProtocol.versionID;
    } else if (protocol.equals(TabletManagerProtocol.class.getName())) {
      return TabletManagerProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to tablet server: " + protocol);
    }
  }

  public boolean checkServer() {
    return true;
  }

  public void splitForTest(TabletInfo tabletInfo) throws IOException {
    Tablet tablet = getTablet(tabletInfo.getTabletName());
    if (tablet == null) {
      LOG.debug("splitForTest:[" + tabletInfo.getTabletName() + "] not serviced on this server(" + hostName + ")");
    }
    if (tablet.isReady()) {
      tablet.doTabletSplit();
    }
  }
  
  public void stopAction(String tableName, String tabletActionClassName) throws IOException {
    try {
      LOG.info("stopAction:" + tableName + "," + tabletActionClassName);
      for(Tablet eachTablet: tablets.values()) {
        TabletInfo tabletInfo = eachTablet.getTabletInfo();
        if(tabletInfo.getTableName().equals(tableName)) {
          TabletAction tabletAction = (TabletAction)Class.forName(tabletActionClassName).newInstance();
          eachTablet.actionChecker.endActionType(tabletAction);
          
          if(BatchUploadAction.class.getName().equals(tabletActionClassName)) {
            LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.UPLOADER + "/" + tabletInfo.getTabletName()), true);
            batchUploadActions.remove(tabletInfo.getTabletName());
          }
        }
      }
    } catch (Exception e) {
      throw new IOException("endAction:" + tabletActionClassName + ":" + e.getMessage());
    }
  }
  
  private void exit() {
//    try {
//      final int STACK_DEPTH = 20;
//      boolean contention = threadBean.isThreadContentionMonitoringEnabled();
//      long[] threadIds = threadBean.getAllThreadIds();
//      LOG.info("Process Thread Dump");
//      LOG.info(threadIds.length + " active threads");
//      for (long tid: threadIds) {
//        ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
//        if (info == null) {
//          LOG.info("  Inactive");
//          continue;
//        }
//        LOG.info("Thread " + info.getLockOwnerId() + "," + info.getLockOwnerName());
//        Thread.State state = info.getThreadState();
//        LOG.info("  State: " + state);
//        LOG.info("  Blocked count: " + info.getBlockedCount());
//        LOG.info("  Waited count: " + info.getWaitedCount());
//        if (contention) {
//          LOG.info("  Blocked time: " + info.getBlockedTime());
//          LOG.info("  Waited time: " + info.getWaitedTime());
//        }
//        if (state == Thread.State.WAITING) {
//          LOG.info("  Waiting on " + info.getLockName());
//        } else  if (state == Thread.State.BLOCKED) {
//          LOG.info("  Blocked on " + info.getLockName());
//          LOG.info("  Blocked by " + info.getLockOwnerId() + "," + info.getLockOwnerName());
//        }
//        LOG.info("  Stack:");
//        for (StackTraceElement frame: info.getStackTrace()) {
//          LOG.info("    " + frame.toString());
//        }
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
    
    System.exit(-1);
    LOG.info("Called System.exit(-1)");
  }
  
  public void shutdown() {
    //LOG.fatal("Kill Tablet Server:" + hostName + ",freeMemory:" + SizeOf.freeMemory() + "/" + SizeOf.totalMemory());
    LOG.fatal("Kill Tablet Server:" + hostName + "," + testMode);
    if(!testMode) {
//      try {
//        System.out.println("Kill Tablet Server:" + hostName + ",freeMemory:" + SizeOf.freeMemory() + "/" + SizeOf.totalMemory());
//      } catch (Exception e) {
//        LOG.error(e);
//      }
      System.exit(-1);
    } else {
    //if (!stopRequested) {
      stopRequested = true;
      tablets.clear();
      
      try {
        server.stop();
      } catch (Exception e) {
        LOG.error(e);
      }
      try {
        ((DataXceiveServer) this.dataXceiveServer.getRunnable()).kill();
      } catch (Exception e) {
        LOG.error(e);
      }
      
      try {
        if(minorCompactionThread != null) {
          minorCompactionThread.interrupt();
        }
      } catch (Exception e) {
        LOG.error(e);
      }
      try {
        if(majorCompactionThread != null) {
          majorCompactionThread.interrupt();
        }
      } catch (Exception e) {
        LOG.error(e);
      }
      
      try {
        //ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
        Thread[] threads = new Thread[threadGroup.activeCount()];
        threadGroup.enumerate(threads, true);
        
        for(Thread thread: threads) {
          try {
            thread.interrupt();
          } catch (Exception e) {
            
          }
        }       
      } catch (Exception e) {
        LOG.error(e);
      }

      try {
        loadThreadExecutor.shutdownNow();
        compactionExecutor.shutdownNow();
        splitExecutor.shutdownNow();
      } catch (Exception e) {
        LOG.error(e);
      }
      
      try {
        zk.close();
      } catch (Exception e) {
        LOG.error(e);
      }
      
      tabletServerMetrics.shutdown();
      LOG.debug("shutdown tabletserver:" + hostName);
    }
  }
  
  public String startBatchUploader(String tabletName, Row.Key rowKey, boolean touch) throws IOException {
    //FIXME 동일한 Tablet에 대해 이미 batch upload가 수행중인 경우???
    Tablet tablet = getTablet(tabletName);
    if(tablet == null) {
      throw new IOException("[" + tabletName + "] not serviced on this server(" + hostName + ")");
    }
    
    if (rowKey.compareTo(tablet.getTabletInfo().getEndRowKey()) > 0) {
      throw new IOException(rowKey + " is out of range:" + tablet.getTabletInfo());
    }
    //runBatchUpload 중인 경우에는 split/merge가 안되게
    //split, merge 수행중인 경우에는 close(), 클라이언트에서 다시 처리하도록
    BatchUploadAction action = new BatchUploadAction();
    
    synchronized(batchUploadActions) {
      boolean result =  tablet.actionChecker.startAction(action);
      if(result) {
        Map<String, BatchUploadAction> actions = batchUploadActions.get(tabletName);
        if(actions == null) {
          actions = new HashMap<String, BatchUploadAction>();
          batchUploadActions.put(tabletName, actions);
        }
        //TabletServer가 kill 되었을 경우 새로 할당받은 TabletServer에서도 계속 처리 가능하도록 lock 설정
        LockUtil.createNodes(zk, 
            LockUtil.getZKPath(conf, Constants.UPLOADER + "/" + tabletName + "/" + action.getActionKey()), 
            null, CreateMode.PERSISTENT);
        
        actions.put(action.getActionKey(), action);

        String uploaderActionId = action.getActionKey();

        if(touch) {
          int expireTime = conf.getInt("client.tx.timeout", 60) * 2 * 1000;
          leaseHolder.createLease(uploaderActionId, new TabletUploaderListener(tabletName, uploaderActionId), expireTime);
        }
        
        LOG.info("Start BatchUploader:rowKey=" + rowKey + "," + tablet.getTabletInfo());
        return uploaderActionId;
      } else {
        return null;
      }
    }
  }

  public void cancelBatchUploader(String actionId, String tabletName) throws IOException {
    LOG.info("cancelBatchUploader:" + tabletName + "," + actionId);
    Tablet tablet = getTablet(tabletName);
    if(tablet == null ) {
      LOG.warn("[" + tabletName + "] not serviced on this server(" + hostName + ")");
      return;
    }
    
    Map<String, BatchUploadAction> actions = null;
    
    synchronized(batchUploadActions) {
      if(!batchUploadActions.containsKey(tabletName)) {
        LOG.warn("No opened Uploader: " + tabletName);
        return;
      }
      
      actions = batchUploadActions.get(tabletName);
      
      if(!actions.containsKey(actionId)) {
        LOG.warn("No opened Uploader: " + tabletName + "," + actionId + ",host=" + hostName);
        return;
      }

      try {
        LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.UPLOADER + "/" + tabletName + "/" + actionId), true);
      } catch (Exception e) {
        throw new IOException(e);
      }
      tablet.actionChecker.endAction(actions.get(actionId));
      actions.remove(actionId);
      leaseHolder.removeLease(actionId);
    }
  }
  
  public String endBatchUploader(
      String actionId,
      String tabletName, 
      String[] columnNames, 
      String[] mapFileIds,
      String[] mapFilePaths) throws IOException {
    Tablet tablet = getTablet(tabletName);
    if(tablet == null ) {
      LOG.warn("[" + tabletName + "] not serviced on this server(" + hostName + ")");
      return null;
    }
    synchronized(batchUploadActions) {
      if(!batchUploadActions.containsKey(tabletName)) {
        LOG.warn("No opened Uploader: " + tabletName);
        return null;
      }
      
      if(!batchUploadActions.get(tabletName).containsKey(actionId)) {
        LOG.warn("No opened Uploader: " + tabletName + "," + actionId + ",host=" + hostName);
        return null;
      }
    }
    
    String taskId = asyncTaskManager.runAsyncTask(
        new BatchUploadEndTask(actionId, tabletName, columnNames, mapFileIds, mapFilePaths));
    return taskId;
  }
  
  class BatchUploadEndTask extends AsyncTask {
    String actionId;
    String tabletName;
    String[] columnNames; 
    String[] mapFileIds;
    String[] mapFilePaths;
    
    public BatchUploadEndTask(String actionId,
        String tabletName, 
        String[] columnNames, 
        String[] mapFileIds,
        String[] mapFilePaths) {
      this.actionId = actionId;
      this.tabletName = tabletName;
      this.columnNames = columnNames;
      this.mapFileIds = mapFileIds;
      this.mapFilePaths = mapFilePaths;
    }
    
    public void exec() throws Exception {
      LOG.info("Start completing batch upload:" + tabletName + ",actionId=" + actionId);

      Map<String, BatchUploadAction> actions = null;
      synchronized(batchUploadActions) {
        actions = batchUploadActions.get(tabletName);
      }
      
      if(actions == null) {
        throw new IOException("No opened Uploader: " + tabletName);
      }
      
      synchronized(actions) {
        if(!actions.containsKey(actionId)) {
          throw new IOException("No opened Uploader: " + tabletName + "," + actionId + ",host=" + hostName);
        }
        Tablet tablet = getTablet(tabletName);
        if(tablet == null ) {
          throw new IOException("[" + tabletName + "] not serviced on this server(" + hostName + ")");
        }
        long indexSize = 0;
        List<TabletMapFile> mapFiles = new ArrayList<TabletMapFile>();
        try {
          LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.UPLOADER + "/" + tabletName + "/" + actionId), true);
          
          if(mapFileIds == null || mapFileIds.length == 0) {
            LOG.warn("BatchUploader.close(): No MapFiles " + tabletName);
            return;
          }      

          for(int i = 0; i < mapFileIds.length; i++) {
            if(mapFileIds[i] == null) {
              continue;
            }
            TabletMapFile tabletMapFile = new TabletMapFile(conf,
                                                            CloudataFileSystem.get(conf),
                                                            tablet.getTabletInfo(), 
                                                            columnNames[i], mapFileIds[i],
                                                            new GPath(mapFilePaths[i]),
                                                            tablet.getTable().getNumOfVersion());
            if(!fs.exists(new GPath(mapFilePaths[i], TabletMapFile.DATA_FILE)) ||
                !fs.exists(new GPath(mapFilePaths[i], TabletMapFile.IDX_FILE)))  {
              //Tablet을 재할달 받은 경우 File이 이미 이동 되었을 수도 있음.
              //FIXME 이전 TabletServer에서 data or index 파일 중 하나만 이동된 경우는?
              LOG.warn("End batch uploader: No data file or index file in " + mapFilePaths[i]);
              continue;
            }
            mapFiles.add(tabletMapFile);
            GPath targetPath = new GPath(Tablet.getTabletPath(conf, tablet.getTabletInfo()), 
                                              columnNames[i] + "/" + tabletMapFile.getFileId() + "/");
            tabletMapFile.moveTo(targetPath, true);
            tabletMapFile.loadIndex();
            indexSize += tabletMapFile.getIndexMemorySize();
            LOG.debug("add mapfile:" + tabletMapFile.getFilePath() + "," + tabletMapFile.getFileId());
            tablet.getDiskSSTable().addTabletMapFile(columnNames[i], tabletMapFile);
            
            tablet.getDiskSSTable().printMapFileInfo();
          }
        } catch (Exception e) {
          //map 파일 제거
          for(TabletMapFile eachMapFile: mapFiles) {
            try {
              tablet.getDiskSSTable().removeTabletMapFile(eachMapFile.getColumnName(), eachMapFile);
              eachMapFile.delete();
            } catch (Exception err) {
            }
          }
          LOG.error("endBatchUploader:" + e.getMessage(), e);
          throw new IOException(e.getMessage());
        } finally {
          LOG.debug("End endBatchUploader:" + actionId);
          tablet.actionChecker.endAction(actions.get(actionId));
          actions.remove(actionId);
          mapFiles.clear();
          leaseHolder.removeLease(actionId);
        }
      }
    }
  }
  
  public AsyncTaskStatus getAsyncTaskStatus(String taskId) throws IOException {
    return asyncTaskManager.getAsyncTaskStatus(taskId);
  }
  
  public void truncateColumn(String tabletName, String columnName) throws IOException {
    Tablet tablet = getTablet(tabletName);
    if(tablet == null) {
      throw new IOException("[" + tabletName + "] not serviced on this server(" + hostName + ")");
    }
    
    tablet.truncateColumn(columnName);
  }
  
  public void addColumn(String tableName, String tabletName, String addedColumnName) throws IOException {
    Tablet tablet = getTablet(tabletName);
    if(tablet == null) {
      throw new IOException("[" + tabletName + "] not serviced on this server(" + hostName + ")");
    }
    tablet.addColumn(addedColumnName);
  }
  
  public boolean isShutdowned() {
    return stopRequested;
  }

  class TabletLoadTask implements Runnable {
    private TabletInfo tabletInfo;
    private boolean ready = false;
    
    public TabletLoadTask(TabletInfo tabletInfo) {
      this.tabletInfo = tabletInfo;
    }

    public void run() {
      //Drop 처리 중인 Table인 경우
      String tableDropLockPath = Constants.TABLE_DROP + "/" + tabletInfo.getTableName();
      try {
        if(zk.exists(LockUtil.getZKPath(conf, tableDropLockPath), false) != null) {
          //Drop 처리 중인 Table인 경우
          CTable ctable = CTable.openTable(conf, TABLE_NAME_META);
          tablets.remove(tabletInfo.getTabletName());
          //META에서 Tablet 정보를 삭제한다.
          LOG.debug("Drop table:delete from META:" + tabletInfo);
          Row.Key rowKey = Tablet.generateMetaRowKey(tabletInfo.getTableName(), tabletInfo.getEndRowKey());
          ctable.remove(rowKey, Constants.META_COLUMN_NAME_TABLETINFO, new Cell.Key(tabletInfo.getTabletName()));
          return;
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
      
      LOG.info("TabletLoad start:" + tabletInfo);
      boolean create = false;
      Tablet tablet = null;
      try {
        create = !fs.exists(tabletInfo.getTabletPath());

        LOG.debug("tablet create : " + create);

        if (create) {
          tablet = createNewTablet();
          ready = true;
        } else {
          tablet = loadExistedTablet();    
        }
        
        if(tablet == null) {
          LOG.error("Error while tablet loading: tablet is null: " + tabletInfo);
          return;
        }
        
        //BatchUploader가 수행되어 있는지 확인
        checkBatchUploader(tablet);
        
        tablet.getReady().compareAndSet(false, ready);
        
        //TabletServer의 서비스 목록에 추가
        tablets.put(tabletInfo.getTabletName(), tablet);
      } catch (Exception e) {
        LOG.error("Tablet assign error tablet[" + tabletInfo.getTabletName() + "]", e);
        tablets.remove(tabletInfo.getTabletName());

        reportLoadError();
        return;
      } 

      // Master로 end report
      try {
        connectMasterServer().endTabletAssignment(tabletInfo, create);
        if (tabletDistributionMode.get() && tablets.size() > 1) {
          //LOG.info("Turn off tablet distribution mode");
          tabletDistributionMode.set(false);
        }        
      } catch (Exception e) {
        LOG.error("Error while reporting end of tablet load to master", e);
      }
      
      tabletServerMetrics.numLoadedTablet.incrementAndGet();
    }

    private void reportLoadError() {
      //Master로 error report
      try {
        tabletInfo.setAssignedHostName(hostName);
        connectMasterServer().errorTabletAssignment(hostName, tabletInfo);
      } catch (IOException e1) {
        LOG.error("Tablet assign error: reportLoadError", e1);
      }
    }

    private void checkBatchUploader(Tablet tablet) throws IOException {
      List<String> actionIds = null;
      try {
        actionIds = zk.getChildren(LockUtil.getZKPath(conf, Constants.UPLOADER + "/" + tabletInfo.getTabletName()), false);
      } catch(NoNodeException e) {
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      if(actionIds == null || actionIds.size() == 0) {
        return;
      }
      
      for(String actionId: actionIds) {
        BatchUploadAction action = new BatchUploadAction(actionId);
        
        synchronized(batchUploadActions) {
          boolean result =  tablet.actionChecker.startAction(action);
          if(result) {
            Map<String, BatchUploadAction> actions = batchUploadActions.get(tabletInfo.getTabletName());
            if(actions == null) {
              actions = new HashMap<String, BatchUploadAction>();
              batchUploadActions.put(tabletInfo.getTabletName(), actions);
            }
            actions.put(action.getActionKey(), action);
          }
        }
      }
    }
    
    private boolean lockRootTablet() throws IOException {
      try {
        if(zk.exists(LockUtil.getZKPath(conf, Constants.ROOT_TABLET_HOST), false) != null) {
          return false;
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.ROOT_TABLET_HOST),
          hostName.getBytes(), CreateMode.EPHEMERAL);
      LOG.debug("Save ROOT Host info to " + Constants.ROOT_TABLET_HOST + ", hostName=" + hostName);
      
      return true;
    }
    
    private Tablet createNewTablet() throws IOException {
      // create new tablet
      if(!fs.mkdirs(tabletInfo.getTabletPath())) {
        if(!fs.mkdirs(tabletInfo.getTabletPath())) {
          throw new IOException("Fail make tablet info on filesystem:" + tabletInfo.getTabletPath());
        } 
      }
      
      tabletInfo.setAssignedHostName(hostName);
      //Tablet 생성
      Tablet tablet = null;
      try {
        TableSchema tableSchema = descTable(tabletInfo.getTableName());
        tablet = new Tablet(conf, tableSchema, TabletServer.this, tabletInfo);
      } catch(IOException e) {
        try {
          fs.delete(Tablet.getTabletPath(conf, tabletInfo), true);
        } catch (Exception e1) {}
        LOG.error("Error while create new Tablet:" + tabletInfo, e);
        throw e;
      }
      
      // insert tablet info to META or lock ROOT
      if (TABLE_NAME_ROOT.equals(tabletInfo.getTableName())) {
        if(!lockRootTablet()) {
          LOG.warn("ROOT Tablet already serviced by other TabletServer");
          //이미 다른 Tablet에서 서비스 하고 있는 경우
          return null;
        }
      } else {
        try {
          insertTableInfo(tabletInfo, System.currentTimeMillis());
        } catch (IOException e) {
          //오류 발생시 생성된 파일 삭제
          try {
            fs.delete(Tablet.getTabletPath(conf, tabletInfo), true);
          } catch (Exception e1) {}
          throw e;
        }
      }
      
      return tablet;
    }

    private Tablet loadExistedTablet() throws IOException {
      // load existed tablet
      LOG.debug("Start loadExistedTable: " + tabletInfo);
      
      tabletInfo.setAssignedHostName(hostName);
      Tablet tablet = getTablet(tabletInfo.getTabletName());
      if(tablet != null) {
        //이미 서비스 하고 있는 경우
        LOG.info("The tablet is already served. return");
        return null;
      }

      TableSchema tableSchema = descTable(tabletInfo.getTableName());
      if(tableSchema == null) {
        LOG.error("No Table Schema:" + tabletInfo.getTableName());
        throw new IOException("No Table Schema:" + tabletInfo.getTableName());
      }
      tablet = new Tablet(conf, tableSchema, TabletServer.this, tabletInfo);
      if(!tablet.load()) {
        LOG.warn("Call minor compaction cause fail loading commit log:" + tabletInfo.getTabletName());
        ready = false;
        tablet.doMinorCompaction(true);
      } else {
        ready = true;
      }
      //delete drop lock
      String lockPath = Constants.TABLET_DROP + "/" + 
                          tabletInfo.getTableName() + "/" + tabletInfo.getTabletName();
      
      LockUtil.delete(zk, LockUtil.getZKPath(conf, lockPath), true);
      
      // modify tablet service hostname in metadata
      if (TABLE_NAME_ROOT.equals(tabletInfo.getTableName())) {
        if(!lockRootTablet()) {
          //lock을 가져 올수 없으면 다른 TabletServer에서 서비스하고 있는 경우
          LOG.warn("ROOT Tablet already serviced by other TabletServer");
          return null;
        }
      } else {
        modifyTableInfo(tabletInfo);
      }
      LOG.info("End loadExistedTable: " + tabletInfo);
      return tablet;
    }
  } // end of TabletLoadThread class

  class MinorCompactionCheckThread extends Thread {
    private long maxMemorySSTableSize;
    private long forceCompactionMemory;
    private long maxMemoryPerTablet;
    private long maxMemory;
    private long minMemory;
    private float flushingRatio;
    //private int minorCompactionMaxSize;
    
    private int sleepTime;
    
    public MinorCompactionCheckThread() {
      super("MinorCompactionThread");
      minMemory = conf.getLong("tabletServer.minMemory", 200) * 1024 * 1024;
      maxMemoryPerTablet = conf.getLong("memory.maxTabletSizePerTablet", 30) * 1024L * 1024L;
      maxMemorySSTableSize = conf.getLong("memory.maxTabletSize", 400) * 1024L * 1024L;
      flushingRatio = conf.getFloat("memory.flushing.ratio", 0.4f);
      maxMemory = SizeOf.maxMemory();

      float maxFreeMemoryRate = conf.getFloat("memory.maxFreeMemoryRate", 0.2f);
      forceCompactionMemory = (long)((float)maxMemory * maxFreeMemoryRate);
     
      //minorCompactionMaxSize = conf.getInt("tabletServer.maxMinorCompactionThread", 2);
      
      sleepTime = conf.getInt("tabletServer.minorCompactionTime", 5);
    }

    public void run() {
      while (!stopRequested) {
        try {
          Thread.sleep(sleepTime * 1000);
        } catch (InterruptedException e) {
          return;
        }
        
        try {
          long freeMemory = SizeOf.freeMemory();
          long totalMemory = SizeOf.totalMemory();
          
          //TabletServer의 메모리가 부족한 경우 모든 Tablet에 대해 minor compaction을 수행한다.
          long realFree = maxMemory - (totalMemory - freeMemory); 
          if(realFree < forceCompactionMemory) {
            LOG.warn("Not enough free memory:freeMemory=" + freeMemory + ", maxMemory=" + maxMemory + ", " +
              "realFree=" + realFree + ", forceCompactionMemory=" + forceCompactionMemory + ", " + 	
              "all tablets do minor compaction");
            if(realFree < minMemory) {
              heavyMemory.getAndSet(2);
              LOG.info("Set HeavyMemory true.(2)");
            } else {
              heavyMemory.getAndSet(1);
              LOG.info("Set HeavyMemory true.(1)");
            }
            tabletServerMetrics.numHeaveMemory.incrementAndGet();
            forceMinorCompactionAllTablets();
            continue;
          } else {
            if(heavyMemory.get() > 0) {
              LOG.info("Set HeavyMemory reset to false.");
            }
            heavyMemory.getAndSet(0);
          }
          
          //minor compaction 대상이 되는 tablet과 전체 Tablet의 MemorySSTable 사이즈를 가져온다.
          List<Tablet> compactionTargets = new ArrayList<Tablet>();
          int sumSize = 0;
          
          int greatestSize = 0;
          //Tablet greatestTablet = null;
          
          Enumeration<Tablet> en = tablets.elements();
          while(en.hasMoreElements()) {
            Tablet tablet = en.nextElement();
            int memTableSize = tablet.getMemtableSize();
            
//            if (memTableSize > greatestSize) {
//              greatestTablet = tablet;
//            }
            
            if(memTableSize >= maxMemoryPerTablet && !tablet.isSplitting()) {
              compactionTargets.add(tablet);
            }
            sumSize += memTableSize;
          }
  
          if (sumSize > maxMemorySSTableSize) {
            scheduleMinorCompactionToTopNTablets();
          } else if(compactionTargets.size() > 0) {
            scheduleMinorCompactionToOverburdenTablets(compactionTargets);
          } 
//          else if (checkMinorCompactionToTablet(greatestTablet)) { // exceed 5MB
//            scheduleMinorCompactionTo(greatestTablet);
//          }
        } catch (Exception e) {
          LOG.error("MinorCompactionThread Error:" + e.getMessage(), e);
        }
      }
    }
    
    private void scheduleMinorCompactionToOverburdenTablets(List<Tablet> compactionTargets) 
      throws IOException, InterruptedException {
      for(Tablet eachTablet: compactionTargets) {
        eachTablet.doMinorCompaction();
      }
    }

    private void scheduleMinorCompactionToTopNTablets() throws IOException, InterruptedException {
      Tablet[] targetTablets = findMaxMemoryTablet();
      if(targetTablets == null || targetTablets.length == 0) {
        return;
      }
      
      int count = 0;
      for(Tablet tablet: targetTablets) {
        if(Constants.TABLE_NAME_META.equals(tablet.getTabletInfo().getTableName()) || 
            Constants.TABLE_NAME_ROOT.equals(tablet.getTabletInfo().getTableName())) {
          continue;
        }
        
        try {
          count ++;
          if(count > TabletServer.this.actionExecutor.getPoolSize() - TabletServer.this.actionExecutor.getActiveCount()) {
            break;
          }
          tablet.doMinorCompaction();
        } catch (Exception e) {
          LOG.error("MinorCompaction Error2:" + tablet.getTabletInfo() + "," + e.getMessage(), e);
        }
      }
    }
    
    private void forceMinorCompactionAllTablets() {
      List<Tablet> tabletList = new ArrayList<Tablet>();
      tabletList.addAll(tablets.values());

      Collections.sort(tabletList, new Comparator<Tablet>() {
        public int compare(Tablet tablet1, Tablet tablet2) {
          // DECENDING ORDER
          return tablet2.getMemtableSize() - tablet1.getMemtableSize(); 
        }
      });
      
      int count = 0;
      for(Tablet eachTablet: tabletList) {
        if(Constants.TABLE_NAME_META.equals(eachTablet.getTabletInfo().getTableName()) || 
            Constants.TABLE_NAME_ROOT.equals(eachTablet.getTabletInfo().getTableName())) {
          continue;
        }
        try {
          //minor compaction을 수행한다.
          eachTablet.doMinorCompaction();
          count++;
          
          if(count > TabletServer.this.actionExecutor.getPoolSize()) {
            break;
          }
        } catch (Exception e) {
          LOG.error("MinorCompaction Error:" + eachTablet.getTabletInfo() + "," + e.getMessage(), e);              
        }
      }
    }

    public Tablet[] findMaxMemoryTablet() {
      List<Tablet> tabletList = new ArrayList<Tablet>();
      tabletList.addAll(tablets.values());
      
      Collections.sort(tabletList, new Comparator<Tablet>() {
        public int compare(Tablet tablet1, Tablet tablet2) {
          // DECENDING ORDER
          return tablet2.getMemtableSize() - tablet1.getMemtableSize(); 
        }
      });
      
      List<Tablet> result = new ArrayList<Tablet>();
      
      long currentSize = 0;
      long flushingSize = (long) (flushingRatio * maxMemorySSTableSize);
      for(int i = 0; i < tabletList.size(); i++) {
        Tablet tablet = tabletList.get(i);
        if(tablet.isSplitting()) {
          continue;
        }
        long memTabletSize = tablet.getMemtableSize();
        currentSize += memTabletSize;
        
        if(memTabletSize > 100 * 1024 * 1024) {
          result.add(tabletList.get(i));
        } else {
          if(memTabletSize == 0) {
            break;
          }
          result.add(tabletList.get(i));
          if (currentSize > flushingSize) {
            break;
          }  
          
          if(result.size() > TabletServer.this.actionExecutor.getPoolSize() - TabletServer.this.actionExecutor.getActiveCount()) {
            break;
          }
        }
      }
      
      return result.toArray(new Tablet[0]);
    }
  } // end of MinorCompactionThread class

  class MajorCompactionSplitCheckThread extends Thread {
    int normalMapFileThreshold;
    int sleepTime = 5;
    float splitCompactionRatio;
    
    AtomicInteger numRunningLocks = new AtomicInteger(0);
    
    public MajorCompactionSplitCheckThread() {
      super("MajorCompactionThread");
      normalMapFileThreshold = conf.getInt("tabletServer.normalMapFileThreshold", 4);
      splitCompactionRatio = conf.getFloat("split.majorcompaction.ratio", 0.5f);
      if(splitCompactionRatio >= 1) {
        splitCompactionRatio = 0.9f;
      }
    }
    
    public void run() {
      Map<String, Tablet> tempTablets = new HashMap<String, Tablet>(10);

      while (!stopRequested) {
        tempTablets.clear();
        
        try {
          Thread.sleep(sleepTime * 1000);
        } catch (InterruptedException e) {
          return;
        }

        tempTablets.putAll(tablets);
        
        boolean isIntensivePutSituation = TabletServer.this.isInIntensivePut();
        Tablet[] splitList = checkSplit(tempTablets);
        Tablet[] majorCompactionList = null;
        if(tempTablets.size() > 1) {
          majorCompactionList = checkMajorCompaction(tempTablets, isIntensivePutSituation);
        } else {
          if(tempTablets.size() == 1) {
            Tablet tablet = tempTablets.values().iterator().next();
            if(tablet.getMapFileCount() > 10) {
              majorCompactionList = new Tablet[]{tablet};
            } else {
              majorCompactionList = new Tablet[0];
            }
          } else {
            majorCompactionList = new Tablet[0];
          }
        }
        
        if(splitList.length + majorCompactionList.length == 0) {
          continue;
        }
        try {
          List <String[]> acquiredScLockPath = acquireSplitCompactionLocks(splitList.length + majorCompactionList.length);
        
          if(acquiredScLockPath.size() == 0) {
            //LOG.debug("No lock for Major or split.");
            continue;
          }
          schedule(acquiredScLockPath, splitList, majorCompactionList);
        } catch(IOException e) {
          LOG.warn("Split&Compaction scheduling error", e);
        }
      }
    }

    private void schedule(List <String[]> acquiredScLockPath, Tablet[] splitList, Tablet[] majorCompactionList) {
      int numSplit = (int) Math.round(acquiredScLockPath.size() * this.splitCompactionRatio);
      if(splitList.length == 0) {
        numSplit = 0;
      } else if(majorCompactionList.length == 0) {
        numSplit = acquiredScLockPath.size();
      } 

//      LOG.debug("acquiredScLockPath:"  + acquiredScLockPath.size() + 
//          ", Split Scheduled:" + splitList.length + ", MajorC: " + majorCompactionList.length + ", numSplit:" + numSplit);

      int consumedLocks = 0;
      
      for(int i = 0; i < numSplit && i < splitList.length; i++) {
        final String[] lockInfo = acquiredScLockPath.get(consumedLocks);
        numRunningLocks.incrementAndGet();
        splitList[i].doTabletSplit(new ActionFinalizer() {
          public void finalizing() {
            try {
              numRunningLocks.decrementAndGet();
              LockUtil.releaseLock(zk, LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH + "/" + lockInfo[0]), lockInfo[1]);
              //LOG.debug("Release Split lock2:" + lockInfo[0] + "," + lockInfo[1]);
            } catch (IOException e) {
              LOG.warn("fail to release lock path[" +  lockInfo[0] + "] due to " + e);
            }
          }
        });
        
        consumedLocks++;
      }
      
      int numMajorCompact = acquiredScLockPath.size() - consumedLocks;
      for(int i = 0; i < numMajorCompact && i < majorCompactionList.length; i++) {
        numRunningLocks.incrementAndGet();
        final String[] lockInfo = acquiredScLockPath.get(consumedLocks);
        try {
          majorCompactionList[i].doMajorCompaction(new ActionFinalizer() {
            public void finalizing() {
              try {
                numRunningLocks.decrementAndGet();
                LockUtil.releaseLock(zk, LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH + "/" + lockInfo[0]), lockInfo[1]);
                //LOG.debug("Release Split lock3:" + lockInfo[0] + "," + lockInfo[1]);
              } catch (IOException e) {
                LOG.warn("fail to release lock path[" +  lockInfo[0] + "] due to " + e);
              }
            }
          });

          consumedLocks++;
        } catch(IOException e) {
          LOG.warn("Error in scheduing major compaction of tablet[" 
              + majorCompactionList[i].getTabletInfo() + "]");
        }
      }
      
      for(int i = consumedLocks; i < acquiredScLockPath.size(); i++) {
        String[] lockInfo = acquiredScLockPath.get(i);
        try {
          LockUtil.releaseLock(zk, LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH + "/" + lockInfo[0]), lockInfo[1]);
          //LOG.debug("Release Split lock1:" + lockInfo[0] + "," + lockInfo[1]);
        } catch (IOException e) {
          LOG.warn("fail to release lock pathv[" +  lockInfo[0] + "] due to " + e);
        }
      }
    }

    private Tablet[] checkMajorCompaction(Map<String, Tablet> tempTablets, boolean inIntensivePut) {
      Tablet[] targetTablets = findMaxMapFileTablet(tempTablets);
      ArrayList<Tablet> candidate = new ArrayList<Tablet>();
      
      for (Tablet tablet : targetTablets) {
        if (!tablet.isReady() || 
            tablet.actionChecker.hasAlreadyScheduled(tablet.tabletSplitAction)|| 
            tablet.actionChecker.hasAlreadyScheduled(tablet.majorCompactionAction)) {
          continue;
        }

        if(tablet.needMajorCompaction(inIntensivePut? normalMapFileThreshold : 1)) {
          candidate.add(tablet);
          
          if (candidate.size() >= TabletServer.this.maxMajorCompactionThread) {
            break;
          }
        }
      }
      
      return candidate.toArray(new Tablet[0]);
    }

    private Tablet[] checkSplit(Map<String, Tablet> tempTablets) {
      Tablet[] targetTablets = findMaxSizeTablet(tempTablets);
      ArrayList<Tablet> candidate = new ArrayList<Tablet>();
      
      for (Tablet tablet : targetTablets) {
          if (!tablet.isReady()) {
            continue;
          }

          try {
            if(tablet.needToBeSplited()) {
              candidate.add(tablet);
              
              if (candidate.size() >= (splitExecutor.getPoolSize() - splitExecutor.getActiveCount())) {
                break;
              }
            }
          } catch (IOException e) {
            LOG.warn("Split checking error", e);
          }
      }

      return candidate.toArray(new Tablet[0]);
    }

    // by sangchul
    // acquire pleiades locks to run split or major compactions.
    // If there has been already running split or compaction in this tablet server, 
    // the request to acquire lock is not allowed until the split or compaction ends
    private List<String[]> acquireSplitCompactionLocks(int requiredLocks) throws IOException {
      int totalRun = numRunningLocks.get() + requiredLocks;
      if(totalRun > maxMajorCompactionThread || totalRun > maxSplitThread) {
        requiredLocks = (maxMajorCompactionThread > maxSplitThread ? maxMajorCompactionThread : maxSplitThread)
                          - numRunningLocks.get() ;
      }
      List <String[]> acquiredScLockPath = new ArrayList <String[]>();
      if (requiredLocks <= 0) {
        return acquiredScLockPath;
      }
      
      List<String> scLocks = null;
      try {
        scLocks = zk.getChildren(LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH), false);
      } catch(NoNodeException e) {
      } catch (Exception e) {
        throw new IOException(e);
      }

      if (scLocks == null || scLocks.size() == 0) {
        LOG.warn("No nodes at lock path [" + Constants.SC_LOCK_PATH + "]");
        return acquiredScLockPath;
      }
      
      for(String path : scLocks) {
        String lockKey = LockUtil.acquireLock(zk, LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH + "/" + path));
        if(lockKey != null) {
          //LOG.debug("Get Split lock:" + path + "," + lockKey);
          acquiredScLockPath.add(new String[]{path, lockKey});

          if (acquiredScLockPath.size() >= requiredLocks) {
            return acquiredScLockPath;
          }
        }
      }
      
      return acquiredScLockPath;
    }

    private Tablet[] findMaxMapFileTablet(Map<String, Tablet> tempTablets) {
      List<Tablet> tabletList = new ArrayList<Tablet>();
      tabletList.addAll(tempTablets.values());

      Collections.sort(tabletList, new Comparator<Tablet>() {
        public int compare(Tablet tablet1, Tablet tablet2) {
          // ASCENDING ORDER
          return tablet1.getMapFileCount() - tablet2.getMapFileCount();
        }
      });

      int size = maxMajorCompactionThread * 3; //select candidate tablets
      if(tabletList.size() < size) {
        return tabletList.toArray(new Tablet[]{});
      } else {
        Tablet[] targetTablets = new Tablet[size];
        for(int i = 1; i <= size; i++) {
          targetTablets[i - 1] = tabletList.get(tabletList.size() - i); 
        }
        return targetTablets;  
      }
    }
    
    private Tablet[] findMaxSizeTablet(Map<String, Tablet> tempTablets) {
      List<Tablet> tabletList = new ArrayList<Tablet>();
      tabletList.addAll(tempTablets.values());

      Collections.sort(tabletList, new Comparator<Tablet>() {
        public int compare(Tablet tablet1, Tablet tablet2) {
          // ASCENDING ORDER
          return (int) (tablet1.getTabletSize() - tablet2.getTabletSize());
        }
      });

      int size = maxSplitThread * 2;
      if(tabletList.size() < size) {
        return tabletList.toArray(new Tablet[]{});
      } else {
        Tablet[] targetTablets = new Tablet[size];
        for(int i = 1; i <= size; i++) {
          targetTablets[i - 1] = tabletList.get(tabletList.size() - i);  
        }
        return targetTablets;  
      }
    }
  } // end of MajorCompatcionThread class

  class HeartbeatThread extends Thread {
    long minMemory;
    public void run() {
      minMemory = conf.getLong("tabletServer.minMemory", 200) * 1024 * 1024;
      while(true) {
        try {
          String masterHostName = CloudataMaster.getMasterServerHostName(conf, zk);
          TabletMasterProtocol masterServer = (TabletMasterProtocol) CRPC.getProxy(TabletMasterProtocol.class,
                TabletMasterProtocol.versionID, NetworkUtil.getAddress(masterHostName), conf);
          masterServer.heartbeatTS(hostName, tablets.size());
        } catch (Exception e) {
          LOG.error("Can't send heartbeat to CloudataMaster: " + hostName + ": " + e.getMessage(), e);
        }
        try {
          sleep(1000);
        } catch (InterruptedException e) {
          LOG.error(e);
          return;
        }
        long freeMemory = SizeOf.freeMemory();
        long totalMemory = SizeOf.totalMemory();
        long maxMemory = SizeOf.maxMemory();
        //TabletServer의 메모리가 부족한 경우 모든 Tablet에 대해 minor compaction을 수행한다.
        long realFree = SizeOf.maxMemory() - (totalMemory - freeMemory); 
        if(realFree < minMemory) {
          LOG.warn("Not enough free memory:freeMemory=" + freeMemory + 
              ", maxMemory=" + maxMemory + ", realFree=" + realFree + ", minMemory=" + minMemory);
          heavyMemory.getAndSet(2);
        } else if(realFree > minMemory * 4 * 1024 * 1024) {
          heavyMemory.getAndSet(0);
        }
      }
    }
  }
  
  class CommitLogErrorMonitor extends Thread {
    Set<String> errorTablets = new HashSet<String>();
    boolean requestStop = false;
      
    public void run() {
      while(!requestStop) {
        List<String> tmpTablets = null;
        synchronized(errorTablets) {
          if(errorTablets.isEmpty()) {
            continue;
          }
          tmpTablets = new ArrayList<String>();
          tmpTablets.addAll(errorTablets);
        }
        
        for(String eachTablet: tmpTablets) {
          Tablet tablet = getTablet(eachTablet);
          if(tablet == null) {
            synchronized(eachTablet) {
              errorTablets.remove(eachTablet);
            }
            continue;
          }
          
          try {
            if(tablet.checkCommitLogError()) {
              synchronized(eachTablet) {
                errorTablets.remove(eachTablet);
              }
            }
          } catch (IOException e) {
            LOG.error("[" + eachTablet + "] checkCommitLogError error:" + e.getMessage(), e);
          }
        }
        
         try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
    
    public void addErrorTablets(String tabletName) {
      synchronized(errorTablets) {
        if(!errorTablets.contains(tabletName)) {
          errorTablets.add(tabletName);
          //errorTablets.notifyAll();
        }
      }
    }
  }
  
  class TabletScannerListener implements LeaseListener {
    String tabletName;

    String scannerId;

    public TabletScannerListener(String tabletName, String scannerId) {
      this.tabletName = tabletName;
      this.scannerId = scannerId;
    }

    public void expire() {
      try {
        LOG.info("Scanner expired:" + scannerId);
        closeScanner(scannerId);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  } // end of TabletScannerListener class

  class TabletUploaderListener implements LeaseListener {
    String tabletName;

    String uploaderId;

    public TabletUploaderListener(String tabletName, String uploaderId) {
      this.tabletName = tabletName;
      this.uploaderId = uploaderId;
    }

    public void expire() {
      try {
        LOG.info("Uploader expired:" + uploaderId);
        endBatchUploader(tabletName, uploaderId, null, null, null);
      } catch (IOException e) {
        LOG.error(e);
      }
    }
  } // end of TabletScannerListener class
  
  /**
   * Scanner를 위한 서버
   * @author babokim
   *
   */
  class DataXceiveServer implements Runnable {
      boolean shouldListen = true;
      ServerSocket ss;
      public DataXceiveServer(ServerSocket ss) {
          this.ss = ss;
      }

      /**
       */
      public void run() {
          try {
              while (shouldListen) {
                  Socket s = ss.accept();
                  new Daemon(new DataXceiver(s)).start();
              }
              ss.close();
          } catch (IOException ie) {
              LOG.info("Exiting ScannerXceiveServer due to " + ie.toString());
          }
      }
      
      public void kill() {
          this.shouldListen = false;
          try {
              this.ss.close();
          } catch (IOException iex) {
          }
      }
  }

  private static class Count {
    int value = 0;
    Count(int init) { value = init; }
    synchronized void incr() { value++; }
    synchronized void decr() { value--; }
    synchronized int getValue() { return value; }
    public String toString() { return Integer.toString(value); }
  }
  
  /**
   * Thread for processing incoming/outgoing data stream
   */
  class DataXceiver implements Runnable {
    Socket socket;
    DataOutputStream out;
    DataInputStream in;
    
    public DataXceiver(Socket socket) {
        this.socket = socket;
    }

    public void run() {
      try {
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        
        byte op = (byte)in.read();
        if(op == OPEN_SCANNER) {
          runScan();
        } else {
          LOG.error("DataXceiver wrong op code:" + op);
        }
      } catch (Exception e) {
        LOG.error("DataXceiver run error: " + e.getMessage(), e);        
      } finally {
        //activeDataDaemons.decrementAndGet();
        if(out != null) { try { out.close(); } catch (Exception e) {} }
        if(in != null) { try { in.close(); } catch (Exception e) {} }
        try {
          socket.close();
        } catch (Exception e) {}
        
      }
    }
    
    private void runScan() throws Exception {
      //FIXME Scanner open시 timeout이 걸렸을 떄 ScannerAction을 삭제시켜주는 로직
      scanCount.incr();
      
      //LOG.debug(hostName + " Number of active sacnner daemon: " + scanCount);
      
      String scannerId = null;
      String tabletName = null;
      try {
        tabletName = CWritableUtils.readString(in);
        //LOG.debug(hostName + " Number of active sacnner daemon: " + scanCount + ",tabletName=" + tabletName);
        String columnName = CWritableUtils.readString(in);
        RowFilter scanFilter = new RowFilter();
        scanFilter.readFields(in);
        
        //LOG.debug(hostName + " Number of active sacnner daemon: " + scanCount + ",tabletName=" + tabletName);
        int retry = 0;
        int maxRetry = 30;
        try {
          tablets.get(tabletName);
          while(retry < maxRetry) {
            scannerId = openScanner(tabletName, columnName, scanFilter);
            if(scannerId != null) {
              break;
            }
            retry++;
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e) {
              return;
            }
          }
          if(retry >= maxRetry && scannerId == null) {
            LOG.error("Scanner open timeout");
            out.write(SCANNER_OPEN_FAIL);
            CWritableUtils.writeString(out, "Scanner open timeout");
            out.flush();
            return;
          }
        } catch (Exception e) {
          LOG.error("Scanner open error:" + e.getMessage());
          out.write(SCANNER_OPEN_FAIL);
          CWritableUtils.writeString(out, e.getMessage());
          out.flush();
          return;
        }
        
        out.write(SCANNER_OPEN_SUCCESS);
        CWritableUtils.writeString(out, scannerId);
        out.flush();

        //FIXME 데이터 전송 중에 에러 발생시 클라이언트에서 이를 어떻게 식별할 것인가?
        long nextCount = 0;
        if(scannerId != null) {
          ColumnValue[] columnValues = null;
          while( (columnValues = next(scannerId))  != null ) {
            out.writeInt(columnValues.length);
            out.flush();
            for(int i = 0; i < columnValues.length; i++) {
              columnValues[i].write(out);
            }
            out.flush();
            nextCount++;
            if(nextCount % 10000 == 0) {
              Thread.sleep(50);
            }
            
            if(nextCount % 10000 == 0) {
              LOG.debug(tabletName + " " + nextCount + " scaned");
            }
          }
          out.writeInt(-1);
          out.flush();
        }
      } catch ( SocketException ignored ) {
        
      } catch(Exception e) {
        LOG.error("runScan", e);
      } finally {
        scanCount.decr();
        if(scannerId != null) {
          try {
            closeScanner(scannerId);
          } catch (Exception e) {
            LOG.error(e);
          }
        }
      }
    }
  }

  public void processCommitLogsServerFail(List<String> errorTablets) {
    for(String eachTablet: errorTablets) {
      try { 
        Tablet tablet = tablets.get(eachTablet);
        if(tablet != null) {
          tablet.processCommitLogServerFailed();
        }
      } catch (IOException e) {
        LOG.error("Can't processCommitLogServerFailed:" + eachTablet + "," + e.getMessage(), e);
      }
    }
  }
  
  public void printMeta(String tabletName) throws IOException {
    tablets.get(tabletName).print();
  }
  
  public TabletReport getTabletDetailInfo(String tabletName) throws IOException {
    Tablet tablet = tablets.get(tabletName);
    if(tablet != null) {
      return tablet.getTabletDetailInfo();
    } else {
      return null;
    }
  }
  
  public boolean canUpload() throws IOException {
    if(uploaderCount.value > 2) {
      return false;
    } else {
      return true;
    }
  }
  
  public void doActionForTest(String tabletName, String action) throws IOException {
    final Tablet tablet = tablets.get(tabletName);
    if(tablet == null) {
      throw new IOException("No Tablet:" + tabletName + ", host=" + hostName);
    }
    if("minorCompaction".equals(action)) {
      tablet.doMinorCompaction();
    } else if("majorCompaction".equals(action)) {
      tablet.doMajorCompaction();
    } else if("split".equals(action)) {
      Thread t = new Thread() {
        public void run() {try {
            tablet.doMinorCompaction();
            while(true) {
              if(tablet.diskSSTable.get().getMaxMapFileCount() <= 1) {
                break;
              } else if(tablet.diskSSTable.get().getMaxMapFileCount() > 1) {
                tablet.doMajorCompaction();
              } else {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  return;
                }          
              }
            }
            tablet.doTabletSplit();
        } catch (Exception e) {
          e.printStackTrace(System.out);
        }
        }
      };
      t.start();
    } else {
      throw new IOException("Invalid action:" + action);
    }
  }
  
  public String getTestHandlerKey() {
    return hostName;
  }
  
  public ColumnValue[][] testRPC() {
    ColumnValue[][] result = new ColumnValue[2][];
    
    return result;
  }
  
  public void setProperty(String key, String value) throws IOException {
    conf.set(key, value);
  }

  public String getProperty(String key, String defaultValue) throws IOException {
    return conf.get(key, defaultValue);
  }
  
  public ColumnValue[] getAllMemoryValues(String tabletName, String columnName) throws IOException {
    Tablet tablet = getTablet(tabletName);
    if(tablet == null) {
      return null;
    }
    
    return tablet.getMemorySSTable().getAllMemoryValues(columnName);
  }

  public CloudataConf getConf() {
    return conf;
  }
  
  public String getTabletServerLockPath() {
    return tabletServerLockPath;
  }
  
  public TabletServerStatus getServerStatus() throws IOException {
    TabletServerStatus status = new TabletServerStatus();
    
    status.setServerStartTime(serverStartTime.getTime());
    status.setFreeMemory(Runtime.getRuntime().freeMemory());
    status.setMaxMemory(Runtime.getRuntime().maxMemory());
    status.setTotalMemory(Runtime.getRuntime().totalMemory());
    status.setMemoryCacheSize(currentMemoryCacheSize.get());
    
    status.setNumMajorThread(compactionExecutor.getActiveCount());
    status.setNumSplitThread(splitExecutor.getActiveCount());
    status.setNumEtcThread(actionExecutor.getActiveCount());
    
    status.setCommitLogPipe(PipeBasedCommitLogFileSystem.totalPipeCount.get());
    status.setTxStatistics(tabletServerMetrics.getTxStatistics());
    return status;
  }
  
  public boolean hasValue(String tabletName, String columnName, Row.Key rowKey, Cell.Key cellKey) throws IOException {
    Tablet tablet = getTablet(tabletName);
    if (tablet == null) {
      throw new IOException("no tablet[" + tabletName + "]");
    }
    return tablet.hasValue(columnName, rowKey, cellKey);
  } 
  
  @Override
  public Row.Key[] getSplitedRowKeyRanges(String tabletName, int splitPerTablet) throws IOException {
    Tablet tablet = getTablet(tabletName);
    if (tablet == null) {
      throw new IOException("no tablet[" + tabletName + "]");
    }
    return tablet.getSplitedRowKeyRanges(splitPerTablet);
  }

  private static final long ONE_MINUTE = 1 * 60 * 1000;
  
  public boolean isInIntensivePut() {
    if (System.currentTimeMillis() - lastMinorCompactionTime.get() > ONE_MINUTE) {
      if (intensivePut.getAndSet(false) == true) {
        LOG.debug("Intensive put is set as FALSE since last minor compaction was occurred more then 1 min ago.");
      }
      return false;
    }
    
    return intensivePut.get();
  }

  public void setLastMinorCompactionTime(long bytesPerMilliSec) {
    if (lastMinorCompactionSpeedLock.tryLock()) {
      lastMinorCompactionTime.set(System.currentTimeMillis());
      try {
        boolean inIntensivePut = lastMinorCompactionSpeed.getAndSet(bytesPerMilliSec) < bytesPerMilliSec;
        
        if (!inIntensivePut && intensivePut.get() && (minSpeed * 1.6) < bytesPerMilliSec) {
          inIntensivePut = true;
        }
        
        if (minSpeed > bytesPerMilliSec) {
          minSpeed = bytesPerMilliSec;
        }
        
        if (intensivePut.getAndSet(inIntensivePut) != inIntensivePut) {
          LOG.debug("Intensive put situation : " + inIntensivePut 
              + ", Minor compacting bytesPerSec : " + bytesPerMilliSec);
        }
      } finally {
        lastMinorCompactionSpeedLock.unlock();
      }
    }
  }

  public void doAllTabletMinorCompaction() throws IOException {
    Thread t = new Thread() {
      public void run() {
        LOG.info("Start doAllTabletMinorCompaction");
        while(true) {
          List<Tablet> currentTablets = new ArrayList<Tablet>();
          synchronized(tablets) {
            currentTablets.addAll(tablets.values());
          }
          
          boolean end = true;
          for(Tablet tablet: currentTablets) {
            if(tablet.getMemtableSize() > 0) {
              try {
                end = false;
                tablet.doMinorCompaction();
              } catch (IOException e) {
                LOG.error(tablet.getTabletInfo().getTabletName() + " can't minor compaction:" + e.getMessage(), e);
              }
            }
          }
          
          if(end) {
            break;
          }
        }
        LOG.info("End doAllTabletMinorCompaction");
      }
    };
    
    t.start();
  }
  
  public void electRebalacingTablets(final int targetTabletNumPerTabletServer) throws IOException {
    Thread t = new Thread() {
      public void run() {
        try {
          List<Tablet> currentTablets = new ArrayList<Tablet>();
          synchronized(tablets) {
            currentTablets.addAll(tablets.values());
          }
          
          if(currentTablets.size() == 0) {
            return;
          }
          
          int count = 0;
          for(Tablet tablet: currentTablets) {
            if(tablet.doStop()) {
              synchronized(tablets) {
                tablets.remove(tablet.getTabletInfo().getTabletName());
              }
              count++;
              boolean end = false;
              if(count >= targetTabletNumPerTabletServer) {
                end = true;
              }
              connectMasterServer().doRebalacing(hostName, tablet.getTabletInfo(), end);
              tablet.clearAll();
              if(count >= targetTabletNumPerTabletServer) {
                break;
              }
            }
          }
          
        } catch (Exception e) {
          LOG.error("Error electRebalacingTablets:" + e.getMessage(), e);
        }
      }
    };
    
    t.start();
  }
  
  public String test(long sleepTime, String echo) throws IOException {
    LOG.info("Ping......");
    if(sleepTime > 0) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        return null;
      }
    }
    return echo;
  }

  public ZooKeeper getZooKeeper() {
    return zk;
  }
  

  public boolean checkCacheCapacity(String tabletName, long addedMemorySize) {
    synchronized(currentMemoryCacheSize) {
      if( tabletName.indexOf(Constants.TABLE_NAME_ROOT) < 0 &&
          tabletName.indexOf(Constants.TABLE_NAME_META) < 0 &&
          (currentMemoryCacheSize.get() + addedMemorySize > maxMemoryCacheCapacity) ) {
        return false;
      } else {
        currentMemoryCacheSize.addAndGet(addedMemorySize);
        return true;
      }
    }
  }  
  
  public void removeFromMemoryCache(String tabletName, long memorySize) {
    synchronized(currentMemoryCacheSize) {
      currentMemoryCacheSize.addAndGet(0 - memorySize);
      LOG.info(tabletName + ": " + memorySize + ", currentMemoryCacheSize:" + currentMemoryCacheSize);
    }
  }
  
  public ColumnValue[] getColumnMemoryCacheDatas(String tabletName, String columnName) throws IOException {
    Tablet tablet = getTablet(tabletName);
    
    if (tablet == null || !tablet.isReady()) {
      throw new IOException("no tablet[" + tabletName + "] on " + hostName);
    }
    
    if(tablet.getDiskSSTable().columnMemoryCaches.containsKey(columnName)) {
      return tablet.getDiskSSTable().columnMemoryCaches.get(columnName).columnCollection.getAllValues();
    } else {
      return null;
    }
  }
}
