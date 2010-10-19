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
package org.cloudata.core.master;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.client.TabletLocationCache;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.commitlog.ServerMonitorInfo;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.GlobalConstants;
import org.cloudata.core.common.CStatusHttpServer;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.TableExistsException;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.ipc.AclManager;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.ipc.CRPC.Server;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.fs.CommitLogFileSystem;
import org.cloudata.core.fs.CommitLogFileSystemIF;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.master.metrics.CloudataMasterMetrics;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchemaMap;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.AsyncTask;
import org.cloudata.core.tabletserver.AsyncTaskManager;
import org.cloudata.core.tabletserver.AsyncTaskStatus;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletManagerProtocol;
import org.cloudata.core.tabletserver.TabletServerInfo;



/**
 * Cloudata의 클러스터를 관리하는 Master<p>
 * CloudataMaster의 역할은 Tablet을 할당해주는 역할만 수행한다. 
 * 따라서 CloudataMaster에 장애가 발생해도 데이터 서비스(get, put)은 정상 서비스 가능하다.
 * 
 * @author 김형준
 * 
 */
public class CloudataMaster implements TableManagerProtocol, TabletMasterProtocol, Constants, Runnable, Watcher {
  public static final Log LOG = LogFactory
      .getLog(CloudataMaster.class.getName());

  private CloudataConf conf;

  private CloudataFileSystem fs;

  /**
   * CloudataMaster host name(HOST_NAME:PORT)
   */
  protected String hostName;

  /**
   * 요청을 받아 처리하는 메인 데몬 프로세스
   */
  private Server server;

  /**
   * 테이블 스키마 정보(tableName -> table)
   */
  private TableSchemaMap schemaMap;

  /**
   * 전체 Tablet 목록(tableName -> [tabletName -> tablets])
   */
  protected Map<String, Map<String, TabletInfo>> tabletInfos = new HashMap<String, Map<String, TabletInfo>>(
      100);

  /**
   * 할당되지 않은 Tablet 목록(TabletName -> TabletInfo)
   */
  private Map<String, TabletInfo> unassignedTablets = new HashMap<String, TabletInfo>(
      100);

  /**
   * 할당 진행 중인 Tablet 목록(TabletName -> TabletInfo)
   */
  private Map<String, TabletInfo> assigningTablets = new HashMap<String, TabletInfo>(
      100);

  /**
   * Live한 TabletServer 목록(hostName -> TabletServerInfo
   */
  private Map<String, TabletServerInfo> liveTabletServers = new HashMap<String, TabletServerInfo>(
      10);

  /**
   * 장애가 발생한 TabletServer 목록
   */
  private Set<String> deadTabletServers = new HashSet<String>();
  
  /**
   * 마지막으로 할당된 TabletServer(Tablet 할당 작업시 마지막에 할당된 TabletServer로는 할당 하지 않음)
   */
  private TabletServerInfo lastAssignedTabletServer;

  /**
   * Drop 진행중인 TabletServer 목록(TableName -> TabletServer HostName)
   */
  private Map<String, List<String>> dropingTabletServers = new HashMap<String, List<String>>(
      10);

  /**
   * Master startup시 Root Tablet이 할당 완료 여부
   */
  private AtomicBoolean endRootTableAssignment = new AtomicBoolean(false);

  /**
   * Tablet 관련 변수 추가, 삭제시 lock monitor
   */
  private Object tabletInfoMonitor = new Object();

  /**
   * CloudataMaster 서버 모든 Thread 종료 flag
   */
  private boolean stopRequested = false;

  /**
   * Root, Meta loading이 완료될 경우 true
   */
  private static boolean clusterReady = false;

  private ThreadGroup threadGroup;

  private CStatusHttpServer infoServer;

  public static CloudataMaster cloudataMaster;

  protected Date masterStartTime;

  protected Date masterInitTime;

  protected AsyncTaskManager asyncTaskManager = new AsyncTaskManager();

  private ZooKeeper zk;
  
  private Object masterElectMonitor = new Object();
  
  private boolean masterElected = false;
  
  private Map<String, ServerMonitorInfo> liveCommitLogServers = new HashMap<String, ServerMonitorInfo>();
  
  private Set<String> deadCommitLogServers = new HashSet<String>();
  
  private CloudataMasterMetrics masterMetrics;
  
  private Balancer balancer;

  private int initialTabletAssignCount;

  private int initialTabletEndCount;

  public CloudataMaster() {
    System.setProperty("java.net.preferIPv4Stack", "true");
    masterStartTime = new Date();
    
    cloudataMaster = this;
  }

  public void init(CloudataConf conf) throws IOException {
    InetSocketAddress serverAddress = NetworkUtil.getLocalAddress(conf.getInt(
        "masterServer.port", 7000));
    
    this.hostName = serverAddress.getHostName() + ":" + serverAddress.getPort();

    this.threadGroup = new ThreadGroup("CloudataMaster_" + hostName);

    this.conf = conf;

    this.fs = CloudataFileSystem.get(conf);
    if(this.fs == null) {
      LOG.fatal("FileSystem is not ready. CloudataMaster shutdown");
      shutdown();
    }
    
    this.zk = LockUtil.getZooKeeper(conf, hostName, this);
    this.schemaMap = new TableSchemaMap(conf, zk);
    
    this.server = CRPC.getServer(zk, this, serverAddress
        .getHostName(), serverAddress.getPort(), conf.getInt(
        "masterServer.handler.count", 10), false, conf);

    this.server.start();

    LOG.info("Netune master started at " + hostName);
  }

  /**
   * CloudataMaster main thread
   */
  public void run() {
    try {
      String masterPathNo = createMasterPath();
      while (!stopRequested) {
        LOG.info(hostName + " starts master election");
        if(isMaster(masterPathNo)) {
          break;
        } 
      }

      // Master 초기화 작업
      LOG.info(hostName + " get master lock.");
      masterInit();
    } catch (Exception e) {
      LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
      shutdown();
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
        LOG.info("Shutdown cause lock expired:" + event);
        shutdown();
        break;
      }
    }
  }
  
  private String createMasterPath() throws Exception {
    if(zk.exists(LockUtil.getZKPath(conf, Constants.MASTER), false) == null) {
      try {
        LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.MASTER), null, CreateMode.PERSISTENT);
      } catch (Exception e) {
        LOG.info("Can't create " + LockUtil.getZKPath(conf, Constants.MASTER )+ " lock", e);
      }
    }
    
    String path = LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.MASTER) + "/Leader-", hostName.getBytes(),
        CreateMode.EPHEMERAL_SEQUENTIAL);
    String[] tokens = path.split("/");
    
    return tokens[tokens.length -1];
  }
  
  private boolean isMaster(String masterPathNo) throws Exception {
    List<String> values = null;
    
    try {
      values = zk.getChildren(LockUtil.getZKPath(conf, Constants.MASTER), false);
    } catch(NoNodeException e) {
    }
    
    if(values == null) {
      return false;
    }
    Collections.sort(values);
    
    String leaderNode = values.get(0);
    
    Stat stat = zk.exists(LockUtil.getZKPath(conf, MASTER) + "/" + leaderNode, new MasterNodeDeletedWatcher(leaderNode));
    
    if(stat == null) {
      LOG.info("No master path [" + LockUtil.getZKPath(conf, MASTER)  + "/" + leaderNode + "]");
      return false;
    }
    
    boolean result = masterPathNo.equals(leaderNode);
    
    if(!result) {
      synchronized(masterElectMonitor) {
        LOG.info(hostName + " not master. watch master node [" + leaderNode + "]");
        masterElectMonitor.wait();
      }
    }
    
    return result;
  }
  
  public boolean isMasterElected() {
    return masterElected;
  }
  
  /**
   * Master가 lock을 acquire한 다음의 작업을 수행한다.
   * 
   * @throws IOException
   */
  private void masterInit() throws IOException {
    if(!fs.isReady()) {
      LOG.fatal("FileSystem is not ready. " +
      		"check " + conf.get("cloudata.root") + " directory. CloudataMaster shutdown");
      shutdown();
    }
    masterInitTime = new Date();
    masterElected = true;
    setClusterReady(false);

    masterMetrics = new CloudataMasterMetrics(conf);
    (new Thread(threadGroup, new UpdateMetricsThread())).start();
    
    addLockEventHandler();

    try {
      loadAllTableSchemas(conf);
      synchronized(Constants.SC_LOCK_PATH) {
        LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH), "0".getBytes(), CreateMode.PERSISTENT, true);        
        LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.MC_LOCK_PATH), "0".getBytes(), CreateMode.PERSISTENT, true);        
      }
    } catch (IOException e) {
      LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
      shutdown();
      return;
    }

    Thread rootTabletAssignmentThread = new Thread(threadGroup,
        new RootTabletAssignmentThread());
    rootTabletAssignmentThread.start();

    InetSocketAddress infoServerAddress = NetworkUtil.getAddress(conf.get("masterServer.info.address", "0.0.0.0:57000"));

    try {
      this.infoServer = new CStatusHttpServer("master", infoServerAddress.getHostName(), 
          infoServerAddress.getPort());
      this.infoServer.start();
      LOG.info("Info Http Server started: " + infoServerAddress.toString());
    } catch (Exception e) {
      LOG.warn("Error while info server init:" + e.getMessage());
    }

    // 종료되지 않은 tablet drop에 대한 처리
    List<String> dropTables = null;
    try {
      dropTables = zk.getChildren(LockUtil.getZKPath(conf, Constants.TABLE_DROP), false);
    } catch(NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }
    if (dropTables != null) {
      for (String eachDropTable : dropTables) {
        asyncTaskManager.runAsyncTask(new TableDropTask(eachDropTable));
      }
    }
  }
  
  AtomicInteger scLockNo = new AtomicInteger(0);
  AtomicInteger mcLockNo = new AtomicInteger(0);

  private void addDiskIOLock(String path, String name, AtomicInteger lockNo) throws IOException {
    LockUtil.createNodes(zk, LockUtil.getZKPath(conf, 
        path + "/" + name + "-" + lockNo.incrementAndGet()), 
        "0".getBytes(), CreateMode.PERSISTENT,
        true);
  }

  /**
   * TabletServer로 접속한다.
   * 
   * @param hostName
   * @return
   * @throws IOException
   */
  protected TabletManagerProtocol connectTabletServer(String hostName)
      throws IOException {
    return connectTabletServer(conf, hostName);
  }

  static TabletManagerProtocol connectTabletServer(CloudataConf conf,
      String hostName) throws IOException {
    // LOG.debug("connectTabletServer:hostname=" + hostName);
    TabletManagerProtocol tabletServer = (TabletManagerProtocol) CRPC.getProxy(
        TabletManagerProtocol.class, TabletManagerProtocol.versionID,
        NetworkUtil.getAddress(hostName), conf);

    return tabletServer;
  }

  private TabletManagerProtocol connectTabletServer(
      TabletServerInfo tabletServerInfo) throws IOException {
    return connectTabletServer(tabletServerInfo.getHostName());
  }

  /**
   * ROOT, META 테이블의 생성/로딩 및 각각의 첫 Tablet이 생성/로딩이 완료되는 시점에 ready가 true로 변경된다.
   * 
   * @param ready
   */
  private void setClusterReady(boolean ready) throws IOException {
    clusterReady = ready;
  }

  /**
   * ROOT, META가 모두 로딩된 상태
   * 
   * @return
   * @throws IOException
   */
  public static boolean isClusterReady() throws IOException {
    return clusterReady;
  }

  /**
   * META tablet을 할당한다. META table이 없으면 Table을 생성한다.
   * 
   * @throws IOException
   */
  private void assignMetaTablet(Map<String, TabletInfo> assignedTablets)
      throws IOException {
    // if meta not exists, make meta table
    boolean createdMetaTable = false;
    if (!schemaMap.contains(TABLE_NAME_META)) {
      createTable(GlobalConstants.META_TABLE, null);
      createdMetaTable = true;
    }

    Map<String, TabletInfo> metaTablets = tabletInfos.get(TABLE_NAME_META);

    //LOG.debug("Needed meta creation: " + createdMetaTable);
    if (createdMetaTable && metaTablets.isEmpty()) {
      // if empty, create
      // MaxTableName.MaxRow.Key
      TabletInfo tabletInfo = new TabletInfo(TABLE_NAME_META, Tablet
          .generateTabletName(TABLE_NAME_META), Row.Key.MIN_KEY, Tablet
          .generateMetaRowKey(null, Row.Key.MAX_KEY));

      LOG.debug("Create META tablet:" + tabletInfo);
      synchronized (tabletInfoMonitor) {
        metaTablets.put(tabletInfo.getTabletName(), tabletInfo);
        unassignedTablets.put(tabletInfo.getTabletName(), tabletInfo);
        assignTablet(tabletInfo);
      }
    } else {
      LOG.debug("Load Tablet infos from META");
      List<TabletInfo> tempMetaTablets = new ArrayList<TabletInfo>();
      synchronized (tabletInfoMonitor) {
        tempMetaTablets.addAll(metaTablets.values());
      }
      for (TabletInfo tabletInfo : tempMetaTablets) {
        String tabletName = tabletInfo.getTabletName();

        // 현재 할당 중인 Tablet에 없으면 할당 처리
        if (!assignedTablets.containsKey(tabletName)) {
          synchronized (tabletInfoMonitor) {
            unassignedTablets.put(tabletInfo.getTabletName(), tabletInfo);
            try {
              assignTablet(tabletInfo);
            } catch (IOException e) {
              LOG.warn(e);
              // assignTablet 작업시 오류에 대한 처리 수행
            }
          }
        }
      }
    }
  }

  private void loadAllTableSchemas(CloudataConf conf) throws IOException {
    List<String> tables = null;
    try {
      tables = zk.getChildren(LockUtil.getZKPath(conf, Constants.PATH_SCHEMA), false);
    } catch(NoNodeException e) {
      
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    if(tables == null) {
      return;
    }
    
    ArrayList<TableSchema> tableSchemaList = new ArrayList<TableSchema>();
    
    for (String eachTableName: tables) {
      TableSchema table = TableSchema.loadTableSchema(conf, zk, eachTableName);
      if(table == null) {
        LOG.error("No table schema data:" + eachTableName);
        continue;
      }
      LOG.info(table.getTableName() + " schema info loaded");
      tableSchemaList.add(table);
    }
    
    schemaMap.load(tableSchemaList);
  }

  /**
   * master가 실행되고 있는 서버 정보를 반환한다.
   * 
   * @param conf
   * @return
   */
  public static String getMasterServerHostName(CloudataConf conf, ZooKeeper zk) throws IOException {
    List<String> values = null;
    
    try {
      try {
        values = zk.getChildren(LockUtil.getZKPath(conf, Constants.MASTER), false);
      } catch(NoNodeException e) {
      }
      
      if(values == null || values.size() == 0) {
        return null;
      }
      
      Collections.sort(values);
      
      String leaderNode = values.get(0);
      
      if(zk.exists(LockUtil.getZKPath(conf, MASTER + "/" + leaderNode), false) == null) {
        LOG.info("No master path [" + LockUtil.getZKPath(conf, MASTER)  + "/" + leaderNode + "]");
        return null;
      }
      
      byte[] data; 
      try {
        data = zk.getData(LockUtil.getZKPath(conf, MASTER + "/" + leaderNode), false, null);
      } catch (NoNodeException e) {
        return null;
      }
      if (data == null) {
        return null;
      }
      return new String(data);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * 모든 데이터 및 스키마를 삭제한다.
   * 
   * @throws IOException
   */
  public static void format(CloudataConf conf, boolean force, String mode) throws IOException {
    if (force) {
      callFormat(conf, mode);
    } else {
      System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
      System.out.println("Warning!!!!");
      System.out.println("Format will remove all datas in " + conf.get("cloudata.root") + "," + conf.get("commitlog.image.dir"));
      System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
      System.out.print("Continue format(Y|N): ");
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      String answer = reader.readLine();

      if ("Y".equals(answer)) {
        callFormat(conf, mode);
      } else {
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        System.out.println("format is cancelled");
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
      }
    }
  }

  private static void callFormat(CloudataConf conf, String mode) throws IOException {
    System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
    System.out.println("Start format " + mode);
    ZooKeeper zk = LockUtil.getZooKeeper(conf, "CloudataMaster", null);
    if("file".equals(mode)) {
      doFormatFile(conf, zk);
    } else if("lock".equals(mode)) {
      doFormatLock(conf, zk);
    }
    
    System.out.println("End format " + mode);
    System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
  }

  public static void formatFileSystem(CloudataConf conf)
      throws IOException {
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    if(fs == null) {
      LOG.fatal("Can't format cause FileSystem Error.");
      return;
    }
    if (!fs.delete(new GPath(conf.get("cloudata.root")), true)) {
      LOG.warn("Can't all data file:" + conf.get("cloudata.root"));
    }
    // fs.close();
  }

  private static void doFormatFile(CloudataConf conf, ZooKeeper zk) throws IOException {
    LOG.info("start deleting files");
    formatFileSystem(conf);
    LOG.info("end deleting files");

    CommitLogFileSystemIF commitLogFileSystem = CommitLogFileSystem.getCommitLogFileSystem(conf, null, zk);
    LOG.info("start deleting commitlog");
    commitLogFileSystem.format();
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    if (!fs.delete(new GPath(conf.get("commitlog.image.dir")), true)) {
      LOG.warn("Can't all data file:" + conf.get("commitlog.image.dir"));
    }
    LOG.info("end deleting commitlog");
  }
  
  private static boolean doFormatLock(CloudataConf conf, ZooKeeper zk) throws IOException {
    try {
      if (zk.exists(LockUtil.getZKPath(conf, ""), false) == null) {
      } else {
        List<String> locks = null;
        try {
          locks = zk.getChildren(LockUtil.getZKPath(conf, ""), false);
        } catch(NoNodeException e) {
        }
        
        if(locks != null) {
          for(String eachLock: locks) {
            LOG.debug("Delete exists lock: " + "" + eachLock);
          }
        }
        LockUtil.delete(zk, LockUtil.getZKPath(conf, ""), true);
      }
      LOG.info("end deleting lock");
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    } finally {
      CloudataMaster.addUser(conf, zk, conf.getUserId());
      CloudataMaster.addSuperGroupUser(conf, zk, conf.getUserId());
    }
  }

  private static void releaseLock(CloudataConf conf) throws IOException {
    LOG.info("start release lock");
    ZooKeeper zk = LockUtil.getZooKeeper(conf, "CloudataMaster", null);
    try {
      if (zk.exists(LockUtil.getZKPath(conf, ""), false) == null) {
        return;
      } 
      
      LOG.debug("release exists lock: " + "" + Constants.ROOT_TABLET_HOST);
      
      try {
        zk.delete(LockUtil.getZKPath(conf, Constants.ROOT_TABLET_HOST), -1);
      } catch(NoNodeException e) {
      }
      
      deleteChildrenLock(conf, zk, Constants.SERVER);
      deleteChildrenLock(conf, zk, Constants.COMMITLOG_SERVER);
      deleteChildrenLock(conf, zk, Constants.MASTER);
      deleteChildrenLock(conf, zk, Constants.TABLETSERVER_SPLIT);
      deleteChildrenLock(conf, zk, Constants.MC_LOCK_PATH);
      deleteChildrenLock(conf, zk, Constants.SC_LOCK_PATH);
      
      LOG.info("end release lock");
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }
  
  private static void deleteChildrenLock(CloudataConf conf, ZooKeeper zk, String path) throws IOException {
    try {
      List<String> locks = null;
      try {
        locks = zk.getChildren(LockUtil.getZKPath(conf, path), false);
      } catch(NoNodeException e) {
        return;
      }
      
      if(locks != null) {
        for(String eachLock: locks) {
          String lockPath = LockUtil.getZKPath(conf, path + "/" + eachLock);
          LOG.debug("release exists lock: " + "" + lockPath);
          LockUtil.delete(zk, lockPath, true);
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  /**
   * 
   * @param args
   * @throws IOException
   */
  public static void main(String args[]) throws IOException {
    CloudataConf conf = new CloudataConf();
    
    if (args.length > 0) {
      boolean force = false;
      if(args.length > 1 && args[1].equals("-noPrompt")) {
        force = true;
      }
      if ("-formatFile".equals(args[0])) {
        format(conf, force, "file");
        return;
      } else if ("-formatLock".equals(args[0])) {
        format(conf, force, "lock");
        return;
      } else if ("-releaseLock".equals(args[0])) {
        releaseLock(conf);
        return;
      } else if ("-drop".equals(args[0])) {
        String tableName = args[1];
        forcedDropTable(tableName);
      } else {
        System.out.println("available arguments [-formatFile|-formatLock|-drop]");
      }
      System.exit(0);
    }
    try {
      StringUtils.startupShutdownMessage(CloudataMaster.class, args, LOG);
      
      CloudataMaster masterServer = new CloudataMaster();
      masterServer.init(conf);
      (new Thread(masterServer)).start();
    } catch (Exception e) {
      LOG.fatal("Error while start CloudataMaster:" + e.getMessage(), e);
    }
  }

  /**
   * lock, schema 존재여부 상관없이 drop 처리
   * 
   * @param tableName
   * @throws IOException
   */
  public static void forcedDropTable(String tableName) throws IOException {
    CloudataConf conf = new CloudataConf();
    CloudataFileSystem fs = CloudataFileSystem.get(conf);

    ZooKeeper zk = LockUtil.getZooKeeper(conf, "CloudataMaster", null);

    try {
      LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.TABLE_DROP + "/" + tableName), 
          "0".getBytes(), CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }

    finalizeDrop(null, conf, fs, zk, tableName, true);
  }

  /**
   * 테이블에 컬럼을 추가한다.
   */
/*
  public void addColumn(String tableName, ColumnInfo addedColumnInfo)
      throws IOException {
    AclManager.checkPermission(conf, zk, schemaMap, tableName, "r");
    
    TableSchema tableSchema = schemaMap.get(tableName);
    
    if (tableSchema == null) {
      throw new IOException("Table not exists [" + tableName + "]");
    }

    synchronized(tableSchema) {
      if (tableSchema.getColumnInfos().contains(addedColumnInfo)) {
        throw new IOException("Column already exists [" + addedColumnInfo + "]");
      }
      tableSchema.addColumn(addedColumnInfo);
      try {
        zk.setData(LockUtil.getZKPath(conf, PATH_SCHEMA + "/" + tableName), 
            LockUtil.getBytes(tableSchema), -1);
      } catch (Exception e) {
        tableSchema.removeColumn(addedColumnInfo);
        LOG.error(e.getMessage(), e);
        
        throw new IOException(e);
      }
    }
  }
*/
  public void addColumn(String tableName, String addedColumnName)
      throws IOException {
    AclManager.checkPermission(conf, zk, schemaMap, tableName, "r");
    
    TableSchema tableSchema = schemaMap.get(tableName);
    
    if (tableSchema == null) {
      throw new IOException("Table not exists [" + tableName + "]");
    }

    synchronized(tableSchema) {
      if (tableSchema.getColumns().contains(addedColumnName)) {
        throw new IOException("Column already exists [" + addedColumnName + "]");
      }
      tableSchema.addColumn(addedColumnName);
      try {
        zk.setData(LockUtil.getZKPath(conf, PATH_SCHEMA + "/" + tableName), 
            LockUtil.getBytes(tableSchema), -1);
      } catch (Exception e) {
        tableSchema.removeColumn(addedColumnName);
        LOG.error(e.getMessage(), e);
        
        throw new IOException(e);
      }
    }
  }

  public TabletInfo addTablet(String tableName, Row.Key startRowKey, Row.Key endRowKey)
      throws IOException {
    if (tableName == null || tableName.equals(TABLE_NAME_ROOT)
        || tableName.equals(TABLE_NAME_META)) {
      throw new IOException("Check table name:" + tableName);
    }

    if (endRowKey == null || endRowKey.equals(Row.Key.MAX_KEY)
        || endRowKey.equals(Row.Key.MIN_KEY)) {
      throw new IOException("Check endRowKey:" + endRowKey);
    }

    if (schemaMap.contains(tableName)) {
      throw new IOException("No Table:" + tableName);
    }

    synchronized (tabletInfoMonitor) {
      Row.Key metaRowKey = Tablet.generateMetaRowKey(tableName, endRowKey);
      CTable ctable = CTable.openTable(conf, TABLE_NAME_META);
      if (ctable.hasValue(META_COLUMN_NAME_TABLETINFO, metaRowKey)) {
        throw new IOException("Already exists tablet:" + tableName + ","
            + endRowKey);
      }

      String tabletName = Tablet.generateTabletName(tableName);
      TabletInfo tabletInfo = new TabletInfo(tableName, tabletName, startRowKey, endRowKey);
      assignTablet(tabletInfo);

      return tabletInfo;
    }
  }

  /**
   * 테이블을 생성한다. endRowKeys에 초기에 생성할 tablet에 대한 목록이 있는 경우 tablet을 여러개 생성시키고 그렇지
   * 않는 경우 min ~ max 범위를 저장하는 하나의 tablet만 생성한다.
   * 
   * @param table
   * @param endRowKeys
   */
  public void createTable(TableSchema table, Row.Key[] endRowKeys) throws IOException {
    // make table schema
    String tableName = table.getTableName();
    LOG.debug("Create Table:" + tableName);
    
    String tabletDropLockPath = Constants.TABLET_DROP + "/" + tableName;
    try {
      if (zk.exists(LockUtil.getZKPath(conf, tabletDropLockPath), false) != null) {
        throw new TableExistsException("table [" + tableName + "] dropping");
      }
    } catch (Exception e) {
      throw new IOException(e);
    }    
    
    if (!schemaMap.putIfAbsent(tableName, table)) {
      throw new TableExistsException("already exists table [" + tableName + "]");
    }
    
    table.saveTableSchema(conf, zk);
    
    synchronized (tabletInfoMonitor) {
      tabletInfos.put(tableName, new HashMap<String, TabletInfo>(100));
    }

    // assign default tablet
    // default tablet row range : 0 ~ 무한대
    if (!TABLE_NAME_ROOT.equals(tableName)
        && !TABLE_NAME_META.equals(tableName)) {
      if (endRowKeys == null || endRowKeys.length == 0) {
        endRowKeys = new Row.Key[1];
        endRowKeys[0] = Row.Key.MAX_KEY;
      }
      Row.Key startRowKey = Row.Key.MIN_KEY;
      for (int i = 0; i < endRowKeys.length; i++) {
        String tabletName = Tablet.generateTabletName(tableName);
        TabletInfo tabletInfo = new TabletInfo(tableName, tabletName,
            startRowKey, endRowKeys[i]);
        assignTablet(tabletInfo);
        startRowKey = endRowKeys[i];
      }

      if(!Row.Key.MAX_KEY.equals(endRowKeys[endRowKeys.length - 1])) {
        String tabletName = Tablet.generateTabletName(tableName);
        TabletInfo tabletInfo = new TabletInfo(tableName, tabletName,
            startRowKey, Row.Key.MAX_KEY);
        assignTablet(tabletInfo);
      }
    }
  }

  public boolean assignTablet(TabletInfo tabletInfo) throws IOException {
    List<TabletServerInfo> tabletServers = new ArrayList<TabletServerInfo>();
    synchronized(liveTabletServers) {
      tabletServers.addAll(liveTabletServers.values());
    }
    return assignTablet(tabletInfo, tabletServers);
  }
  
  private boolean assignTablet(TabletInfo tabletInfo, Collection<TabletServerInfo> tabletServers) throws IOException {
    synchronized (tabletInfoMonitor) {
      if (assigningTablets.containsKey(tabletInfo)) {
        LOG.info("Already requested assigning(" + tabletInfo + ")");
        return true;
      }
      unassignedTablets.remove(tabletInfo.getTabletName());
      assigningTablets.put(tabletInfo.getTabletName(), tabletInfo);
    }

    int retry = 0;
    // 할당 가능한 TabletServer가 있을때까지 반복 수행
    while (retry < 5) {
      int runningSize = tabletServers.size();
      if (runningSize == 0) {
        LOG
            .debug("no active tablet server. can't assign tablet: "
                + tabletInfo);
        break;
      }

      TabletServerInfo tabletServerInfo = null;
      try {
        long startTime = System.currentTimeMillis();
        TabletManagerProtocol tabletServer = null;
        while (true) {
          tabletServerInfo = selectTabletServerForAssignment(tabletServers);

          if (tabletServerInfo != null) {
            try {
              tabletServer = connectTabletServer(tabletServerInfo);
              break;
            } catch (IOException e) {
              tabletServers.remove(tabletServerInfo);
            }
          }
          if (System.currentTimeMillis() - startTime > (10 * 1000)) {
            break;
          }
        }
        if (tabletServerInfo == null) {
          LOG.error("Can't find proper tablet server(live TabletServer="
              + runningSize);
          retry++;
          continue;
        }

        try {
          tabletServer.assignTablet(tabletInfo);
        } catch (IOException e) {
          //장애가 발생한 경우 해당 TabletServer에서 할당되었는지 다시 확인
          //FIXME 하나의 Tablet이 하나의 TabletServer에만 할당될 수 있도록 하는 정책 필요
          if(!liveTabletServers.containsKey(tabletServerInfo.getHostName())) {
            LOG.error("Can't assign to " + tabletServerInfo.getHostName());
            tabletServers.remove(tabletServerInfo);
            retry++;
            continue;
          }
          int errorRetry = 0;
          while(true) {
            try {
              TabletInfo assignedTabletInfo = tabletServer.getTabletInfo(tabletInfo.getTabletName());
              if(assignedTabletInfo != null) {
                //정상적으로 할당되었음
                break;
              }
            } catch (IOException err) {
              LOG.warn("Error checking Tablet already assigned:" + tabletInfo + "," + tabletServerInfo);
              Thread.sleep(1000);
              errorRetry++;
              if(errorRetry >= 10) {
                throw new IOException(e);
              }
            }
          }
        }

        tabletInfo.setAssignedHostName(tabletServerInfo.getHostName());

        tabletServerInfo.addNumOfTablets();

        LOG.info("assignTablet: tabletName=" + tabletInfo.getTabletName()
            + ", assignedHost=" + tabletServerInfo.getHostName());
        return true;
      } catch (Exception e) {
        LOG.warn("error while assignment. but retry:" + e.getMessage());
        if (tabletServerInfo != null) {
          LOG.warn("Exception in assigning tablet : "
              + tabletInfo.getTabletName() + " to host : "
              + tabletServerInfo.getHostName() + ". Retry count : "
              + (retry + 1), e);
        } else {
          LOG.warn("Exception in assigning tablet : "
              + tabletInfo.getTabletName() + " to host null. Retry count : "
              + (retry + 1), e);
        }
        retry++;
        try {
          Thread.sleep(1 * 1000);
        } catch (InterruptedException e1) {
        }
      }
    }

    // if assignment fail
    synchronized (tabletInfoMonitor) {
      assigningTablets.remove(tabletInfo.getTabletName());
      unassignedTablets.put(tabletInfo.getTabletName(), tabletInfo);
    }
    LOG.debug("Assignment fail:" + tabletInfo);
    return false;
  }

  private TabletServerInfo selectTabletServerForAssignment(Collection<TabletServerInfo> tabletServers) {
    List<TabletServerInfo> sortedTabletServers = new ArrayList<TabletServerInfo>();
    sortedTabletServers.addAll(tabletServers);
    Collections.sort(sortedTabletServers,
        new TabletServerInfo.TabletServerInfoComparator());

    TabletServerInfo selectedServer = sortedTabletServers.get(0);

    if (lastAssignedTabletServer == null) {
      lastAssignedTabletServer = selectedServer;
      return selectedServer;
    } else {
      synchronized(lastAssignedTabletServer) {
        if (sortedTabletServers.size() > 1
            && selectedServer.equals(lastAssignedTabletServer)) {
          selectedServer = sortedTabletServers.get(1);
          lastAssignedTabletServer = selectedServer;
          return selectedServer;
        } else {
          lastAssignedTabletServer = selectedServer;
          return selectedServer;
        }
      }
    }
  }

  public AsyncTaskStatus getAsyncTaskStatus(String taskId) throws IOException {
    return asyncTaskManager.getAsyncTaskStatus(taskId);
  }

  public void removeAsyncTask(String taskId) throws IOException {
    asyncTaskManager.removeAsyncTask(taskId);
  }

  public String dropTable(String tableName) throws IOException {
    AclManager.checkOwner(conf, zk, schemaMap, tableName);
    String taskId = asyncTaskManager.runAsyncTask(new TableDropTask(tableName));
    return taskId;
  }

  class TableDropTask extends AsyncTask {
    String tableName;

    public TableDropTask(String tableName) {
      this.tableName = tableName;
    }

    // FIXME Drop 도중에 schema 정보는 삭제 했는데 META에 삭제되지 않은 경우 처리
    public void exec() throws Exception {
      LOG.debug("Drop table:" + tableName);

      synchronized (dropingTabletServers) {
        if (dropingTabletServers.containsKey(tableName)
            && dropingTabletServers.get(tableName).size() > 0) {
          throw new IOException("Already dropping:" + tableName);
        }
      }

      String lockPath = Constants.TABLE_DROP + "/" + tableName;

      if(zk.exists(LockUtil.getZKPath(conf, lockPath), false) != null) {
        throw new IOException("Can't drop table:" + tableName
            + "(already drop lock exists:" + lockPath + ")");
      } else {
        LockUtil.createNodes(zk, LockUtil.getZKPath(conf, lockPath), "0".getBytes(), CreateMode.PERSISTENT);
      }

      // Live한 TabletServer에 drop 요청
      synchronized (dropingTabletServers) {
        List<String> dropTabletServerList = new ArrayList<String>();
        for (String hostName : liveTabletServers.keySet()) {
          TabletManagerProtocol tabletServer = null;
          try {
            tabletServer = connectTabletServer(hostName);
            if (tabletServer.dropTable(getTaskId(), tableName)) {
              dropTabletServerList.add(hostName);
            }
          } catch (IOException e) {
            LOG.fatal("TabletServer[" + hostName + "] can't drop table:"
                + tableName + "." + " shutdown tabletserver", e);
            try {
              tabletServer.shutdown();
            } catch (IOException err) {
              LOG.error("Can't shutdown tabletserver:" + hostName, err);
            }
          }
        }
        
        // 모든 TabletServer에서 처리가 완료될때까지 기다린다.
        // TODO 하나의 테이블이 drop 처리 중인 경우 다른 테이블은 Drop 되지 않는다.
        dropingTabletServers.put(tableName, dropTabletServerList);
        long startTime = System.currentTimeMillis();
        while (true) {
          dropingTabletServers.wait(10 * 1000);

          if (dropingTabletServers.get(tableName) == null) {
            break;
          }
          if (dropingTabletServers.get(tableName).isEmpty()) {
            dropingTabletServers.remove(tableName);
            break;
          }

          // 5 min
	  /*
          long gap = System.currentTimeMillis() - startTime;
          if (gap > 5 * 60 * 1000) {
            LOG.error(tableName + " Drop timeout after " + gap);
            addErrorTrace(tableName + " Drop timeout after " + gap);
            dropingTabletServers.remove(tableName);
            break;
          }
	  */
        }
      }

      String errorMessage = finalizeDrop(CloudataMaster.this, conf, fs,
          zk, tableName, false);
      if (errorMessage != null && errorMessage.length() > 0) {
        addErrorTrace(errorMessage);
      }
      TabletLocationCache.getInstance(conf).removeTableSchemaCache(tableName);
      
      LOG.debug("Drop table end:" + tableName);
    }
  }

  public static String finalizeDrop(CloudataMaster cloudataMaster,
      CloudataConf conf, CloudataFileSystem fs, ZooKeeper zk,
      String tableName, boolean forced) throws IOException {
    String errorMessage = "";

    // Schema 정보 삭제
    try {
      LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.PATH_SCHEMA + "/" + tableName), true);
    } catch (Exception e) {
      if(forced) {
        errorMessage += "Can't delete table schema:" + Constants.PATH_SCHEMA + "/" + tableName + "\n";
      } else {
        throw new IOException(e);
      }
    }
    
    // Table 데이터를 Trash로 rename
    String tableDataPath = TableSchema.getTableDataPath(conf, tableName);
    if (fs.exists(new GPath(tableDataPath))) {
      String tableTrashPath = TableSchema.getTableDataTrashPath(conf, tableName);
      GPath dataTrashPath = new GPath(tableTrashPath);
      fs.mkdirs(dataTrashPath.getParent());
      boolean renameToResult = fs.renameTo(new GPath(tableDataPath),
          dataTrashPath);
      if (!renameToResult) {
        LOG.error("Can't rename table file:" + tableDataPath + " to "
            + tableTrashPath + " : " + renameToResult);
        // LOG.fatal("CloudataMaster shutdown cause: Can't move table data file to trash while droping");
        if(!"local".equals(conf.get("cloudata.filesystem"))) {
          errorMessage += "Can't rename table file:" + tableDataPath + " to "
              + tableTrashPath + " : " + renameToResult + "\n";
        }
      }
    }
    // TODO CommitLog도 삭제하지 않고 Trash로 이동

    // META 정보를 이용하여 처리되지 않은 Tablet의 META 삭제
    CTable ctable = CTable.openTable(conf, TABLE_NAME_META);
    TableScanner scanner = null;
    List<ScanCell> scanCells = new ArrayList<ScanCell>();
    try {
      scanner = ScannerFactory.openScanner(ctable, Tablet.generateMetaRowKey(
          tableName, Row.Key.MIN_KEY), Tablet.generateMetaRowKey(tableName,
          Row.Key.MAX_KEY), META_COLUMN_NAME_TABLETINFO, 20);

      ScanCell scanCell = null;
      while ((scanCell = scanner.next()) != null) {
        TabletInfo tabletInfo = new TabletInfo();
        tabletInfo.readFields(scanCell.getBytes());

        if (tableName.compareTo(tabletInfo.getTableName()) < 0) {
          break;
        }
        if (tableName.equals(tabletInfo.getTableName())) {
          scanCells.add(scanCell);
        }
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }

    if (scanCells != null) {
      for (ScanCell eachCell : scanCells) {
        TabletInfo tabletInfo = new TabletInfo();
        tabletInfo.readFields(eachCell.getBytes());

        try {
          if (tabletInfo.getAssignedHostName() != null) {
            if (CloudataMaster.connectTabletServer(conf,
                tabletInfo.getAssignedHostName()).dropTable(null, tableName)) {
              continue;
            }
          }
          // TabletServer에서 삭제 못시킨 내역만 META에서 삭제.
          Row.Key rowKey = Tablet.generateMetaRowKey(tabletInfo.getTableName(),
              tabletInfo.getEndRowKey());
          ctable.remove(rowKey, Constants.META_COLUMN_NAME_TABLETINFO,
              new Cell.Key(tabletInfo.getTabletName()));

          CommitLogFileSystemIF commitLogFileSystem = CommitLogFileSystem
              .getCommitLogFileSystem(conf, null, zk);

          Tablet.deleteCommitLog(commitLogFileSystem,
              tabletInfo.getTableName(), tabletInfo.getTabletName());
        } catch (IOException err) {
          LOG.error("Can't delet META while table drop:" + tabletInfo + ":"
              + err.getMessage(), err);
          errorMessage += "Can't delet META while table drop:" + tabletInfo
              + ":" + err.getMessage() + "\n";
        }
      }
    }

    // drop lock 제거
    String tableDropLockPath = Constants.TABLE_DROP + "/" + tableName;

    try {
      if (zk.exists(LockUtil.getZKPath(conf, tableDropLockPath), false) != null) {
        LockUtil.delete(zk, LockUtil.getZKPath(conf, tableDropLockPath));
      }
    } catch (Exception e) {
      if(!forced) {
        throw new IOException(e);
      }
    }

    // Master에서 schema 삭제
    if (cloudataMaster != null) {
      cloudataMaster.schemaMap.remove(tableName);
    }

    LOG.debug("End finalizeDrop:" + tableName);

    return errorMessage;
  }

  public void endTableDrop(String taskId, String hostName, String tableName)
      throws IOException {
    synchronized (dropingTabletServers) {
      LOG.debug("Receive endTableDrop from tabletserver:" + hostName + ","
          + tableName);
      List<String> servers = dropingTabletServers.get(tableName);
      if (servers != null) {
        servers.remove(hostName);
      }
      dropingTabletServers.notifyAll();
    }
  }

  public void errorTableDrop(String taskId, String hostName, String tableName,
      String message) throws IOException {
    synchronized (dropingTabletServers) {
      LOG.debug("Receive errorTableDrop from tabletserver:" + hostName + ","
          + tableName);
      AsyncTask task = asyncTaskManager.getAsyncTask(taskId);
      if (task != null) {
        task.addErrorTrace(hostName + ":" + message);
      }

      List<String> servers = dropingTabletServers.get(tableName);
      if (server != null) {
        servers.remove(hostName);
      }
      dropingTabletServers.notifyAll();
    }
  }

  public TabletInfo[] getTablets(String tableName) {
    synchronized (tabletInfoMonitor) {
      Map<String, TabletInfo> tablets = tabletInfos.get(tableName);
      if (tablets == null)
        return null;

      Collection<TabletInfo> tabletInfoList = tablets.values();
      return tabletInfoList.toArray(new TabletInfo[tabletInfoList.size()]);
    }
  }

  public TableSchema[] listTables() {
    Collection<TableSchema> tableList = schemaMap.values();

    return tableList.toArray(new TableSchema[tableList.size()]);
  }

  public void endTabletAssignment(TabletInfo tabletInfo, boolean created) throws IOException {
    // FIXME 처리 중 오류가 발생하면 어떻게 되나??
     LOG.info("endTabletAssignment:" + tabletInfo + ", created:" + created + "," + tabletInfo.getAssignedHostName());
    if (!assigningTablets.containsKey(tabletInfo.getTabletName())) {
      // assigningTablets에 없는 경우는 Tablet이 split된 경우이기 때문에 아무런 처리를 하지 않는다.
      LOG
          .info("receive endTabletAssignment for tablet which is not assigningTablet");
      return;
    }

    synchronized (tabletInfoMonitor) {
      Map<String, TabletInfo> tabletInfoMap = tabletInfos.get(tabletInfo
          .getTableName());
      if (tabletInfoMap == null) {
        tabletInfoMap = new HashMap<String, TabletInfo>(50);
        tabletInfos.put(tabletInfo.getTableName(), tabletInfoMap);
      }
      tabletInfoMap.put(tabletInfo.getTabletName(), tabletInfo);

      unassignedTablets.remove(tabletInfo.getTabletName());
      assigningTablets.remove(tabletInfo.getTabletName());
      // tsReportedTabletInfos.put(tabletInfo.getTableName(), tabletInfo);
      initialTabletEndCount++;
    }

    if (TABLE_NAME_ROOT.equals(tabletInfo.getTableName())) {
      synchronized(endRootTableAssignment) {
        endRootTableAssignment.set(true);
        endRootTableAssignment.notifyAll();
      }
    } else if (TABLE_NAME_META.equals(tabletInfo.getTableName())) {
      // FIXME META tablet이 여러개로 split된 경우
      // if (!clusterReady) {
      // setClusterReady(true);
      // // 클러스터가 처음 시작되어 ROOT -> META -> User Table 순서로 할당 되는 경우에만 수행
      // UserTabletAssignmentThread userTabletAssignmentThread = new
      // UserTabletAssignmentThread(tabletInfo);
      // userTabletAssignmentThread.start();
      // //addRunningThread(userTabletAssignmentThread);
      // }
    }
  }

  /**
   * Tablet할당 에러 발생시 다음과 같이 처리한다. 1. Tablet을 다른 서버로 이관하는 도중에 발생한 에러의 경우 기존 서버에
   * stop service가 요청되어 있는 상태인데 stop server를 해제시키고 계속 서비스 하도록한다.cancelStop 요청에도
   * 에러가 발생한 경우 unsigned tablet 목록에 추가한다. 2. 새로 생성된 Tablet인 경우 Tablet을 unsigned
   * tablet 목록에 추가한다.
   */
  public void errorTabletAssignment(String hostName, TabletInfo tabletInfo) {
    LOG.info("errorTabletAssignment:" + tabletInfo);

    if (TABLE_NAME_ROOT.equals(tabletInfo.getTableName())) {
      LOG
          .fatal("CloudataMaster shutdown cause:CloudataMaster stop cause by ROOT tablet assignment fail");
      shutdown();
      return;
    }

    if (!assigningTablets.containsKey(tabletInfo.getTabletName())) {
      // assigningTablets에 없는 경우는 Tablet이 split된 경우이기 때문에 아무런 처리를 하지 않는다.
      LOG
          .error("receive errorTabletAssignment for tablet which is not assigningTablet");
      return;
    }

    synchronized (tabletInfoMonitor) {
      TabletServerInfo tabletServerInfo = liveTabletServers.get(hostName);
	
      if (tabletServerInfo != null) {
        tabletServerInfo.subtractNumOfTablets();
      }
	
      
      assigningTablets.remove(tabletInfo.getTabletName());
      if(!schemaMap.contains(tabletInfo.getTableName())) {
        LOG.warn("Garbage tablet:" + tabletInfo);
        return;
      } else {
        unassignedTablets.put(tabletInfo.getTabletName(), tabletInfo);
        LOG.error("Added unassignedTablets:" + tabletInfo);
      }
    }
  }

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(TableManagerProtocol.class.getName())) {
      return TableManagerProtocol.versionID;
    } else if (protocol.equals(TabletMasterProtocol.class.getName())) {
      return TabletMasterProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to tablet server: " + protocol);
    }
  }

  public boolean checkServer() {
    return true;
  }

  /**
   * 모든 TabletServe로부터 서비스하고 있는 Tablet 목록을 받는다.
   */
  private List<TabletInfo> getTabletsFromTabletServer() throws IOException {
    List<TabletInfo> tabletInfos = new ArrayList<TabletInfo>();

    List<String> servers = null;
    try {
      servers = zk.getChildren(LockUtil.getZKPath(conf, Constants.SERVER), false);
    } catch(NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (servers == null) {
      return tabletInfos;
    }

    for (String tabletServerHost : servers) {
      try {
        TabletManagerProtocol tabletServer = connectTabletServer(tabletServerHost);
        TabletInfo[] tsTabletInfos = tabletServer.reportTablets();
        for (TabletInfo tabletInfo : tsTabletInfos) {
          tabletInfos.add(tabletInfo);
        }
        synchronized (liveTabletServers) {
          TabletServerInfo tabletServerInfo = liveTabletServers.get(tabletServerHost);
          if(tabletServerInfo == null) {
            tabletServerInfo = new TabletServerInfo(tabletServerHost);
            liveTabletServers.put(tabletServerHost, tabletServerInfo);
          }
          tabletServerInfo.setNumOfTablets(tsTabletInfos.length);
        }
      } catch (Exception e) {
        LOG.error("Can't receive tablet reports from:" + tabletServerHost, e);
      }
    }

    return tabletInfos;
  }

  public void reportTabletSplited(final TabletInfo targetTablet,
      final TabletInfo[] splitedTablets) {
    synchronized (tabletInfoMonitor) {
      try {
        removeTabletInAllVariable(targetTablet);

        Map<String, TabletInfo> tablets = tabletInfos.get(splitedTablets[0]
            .getTableName());
        if (tablets == null) {
          tablets = new HashMap<String, TabletInfo>(100);
          tabletInfos.put(splitedTablets[0].getTableName(), tablets);
        }
        tablets.put(splitedTablets[0].getTabletName(), splitedTablets[0]);
        tablets.put(splitedTablets[1].getTabletName(), splitedTablets[1]);
      } catch (Exception e) {
        LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
        shutdown();
        return;
      }
    }

    Thread assignThread = new Thread(threadGroup, new Runnable() {
      public void run() {
        try {
          // 이전에 서비스하던 tablet server에서 stop이 완료된 후에 로딩 & 서비스하는 경우 false 전달
          assignTablet(splitedTablets[0]);
        } catch (Exception e) {
          // 예외가 발생할 경우 Master에서는 주기적으로 돌면서 로드 밸런싱 처리.
        } finally {
          // removeRunningThread(this);
        }
      }
    });
    assignThread.start();
  }

  private void removeTabletInAllVariable(TabletInfo tabletInfo) {
    Map<String, TabletInfo> tablets = tabletInfos
        .get(tabletInfo.getTableName());
    if (tablets != null) {
      tablets.remove(tabletInfo.getTabletName());
    }
    unassignedTablets.remove(tabletInfo.getTabletName());
    assigningTablets.remove(tabletInfo.getTabletName());
  }

  public void shutdown() {
    if (!conf.getBoolean("testmode", false)) {
      System.exit(0);
    }

    if (!stopRequested) {
      stopRequested = true;
      server.stop();

      if (infoServer != null) {
        try {
          infoServer.stop();
        } catch (InterruptedException e1) {
        }
      }

      // ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
      Thread[] threads = new Thread[threadGroup.activeCount()];
      threadGroup.enumerate(threads, true);

      for (Thread thread : threads) {
        try {
          // LOG.debug("Master shutdown:child thread:" + thread.getId());
          thread.interrupt();
        } catch (Exception e) {
        }
      }
    }

    LOG.debug("shutdown masterserver:" + hostName);
  }

  public boolean isShutdowned() {
    return stopRequested;
  }

  /**
   * Master가 시작될 때 Root, Meta Tablet을 할당하고 Tablet 정보를 구성한다.
   * 
   * @author babokim
   */
  class RootTabletAssignmentThread implements Runnable {
    private void assignRootTablet() throws IOException {
      // if root not exists, make root table
      TabletInfo rootTabletInfo = null;
      if(schemaMap.contains(TABLE_NAME_ROOT)) {
        //TabletInfo 로딩
        LOG.info("loading root tablet info");
        rootTabletInfo = Tablet.getRootTabletInfo(conf, zk);
      }
      else {
        LOG.info("create ROOT Tablet");
        // ROOT user 존재확인 후 존재하지 않으면 생성
        if (!existSuperGroup()) {
          LOG.info("Create supergroup user(" + conf.getUserId() + ")");
          addUser(conf, zk, conf.getUserId());
          addSuperGroupUser(conf, zk, conf.getUserId());
        }

        // 존재하지 않으면 생성
        createTable(GlobalConstants.ROOT_TABLE, null);
        rootTabletInfo = new TabletInfo(TABLE_NAME_ROOT, Tablet
            .generateTabletName(TABLE_NAME_ROOT), Row.Key.MIN_KEY,
            Row.Key.MAX_KEY);
        
        try {
          LockUtil.createNodes(zk , LockUtil.getZKPath(conf, Constants.ROOT_TABLET), 
              LockUtil.getBytes(rootTabletInfo),
              CreateMode.PERSISTENT);
        } catch (Exception e) {
          LOG.fatal("CloudataMaster shutdown cause:Fail ROOT tablet lock:"
              + Constants.ROOT_TABLET);
          shutdown();
          return;
        }
      }

      if (rootTabletInfo == null) {
        LOG.fatal("CloudataMaster shutdown cause:Can't get ROOT tablet info");
        shutdown();
        return;
      }

      //wait for starting TabletServer
      synchronized (liveTabletServers) {
        if (liveTabletServers.isEmpty()) {
          LOG.info("No live tablet server. wait until tablet server added.");
          try {
            liveTabletServers.wait();
          } catch (InterruptedException e) {
          }
          LOG.info("TabletServer added: # of TabletServers: " + liveTabletServers.size());
        }
      }

      synchronized (tabletInfoMonitor) {
        Map<String, TabletInfo> rootTablets = new HashMap<String, TabletInfo>();
        rootTablets.put(rootTabletInfo.getTabletName(), rootTabletInfo);
        tabletInfos.put(TABLE_NAME_ROOT, rootTablets);
      }

      // TabletServer는 stop 되지 않고 Master만 kill 또는 stop 된 경우
      // ROOT Tablet이 서비스 중인지 확인한다.
      // 서비스 중이 아니면 다시 할당한다.
      boolean exists;
      try {
        exists = zk.exists(LockUtil.getZKPath(conf, Constants.ROOT_TABLET_HOST), false) != null;
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      if(exists) {
        synchronized(endRootTableAssignment) {
          endRootTableAssignment.set(true);
          endRootTableAssignment.notifyAll();
        }
      } else {
        unassignedTablets.put(rootTabletInfo.getTabletName(), rootTabletInfo);
        assignTablet(rootTabletInfo);
      }
    }
    
    public void run() {
      // assignment root tablet
      try {
        assignRootTablet();
      } catch (Exception e) {
        LOG.fatal(e);
        shutdown();
      }

      // check finished root tablet assignment
      synchronized(endRootTableAssignment) {
        if(!endRootTableAssignment.get()) {
          LOG.info("wait until ROOT tablet assigned");
          try {
            endRootTableAssignment.wait();
          } catch (InterruptedException e) {
          }
          LOG.info("ROOT tablet assigned.");
        }
      }

      // report from tablet server. find unassigned tablet
      Map<String, TabletInfo> assignedTablets = new HashMap<String, TabletInfo>();
      try {
        List<TabletInfo> tsTabletInfos = getTabletsFromTabletServer();
        for (TabletInfo eachTabletInfo : tsTabletInfos) {
          assignedTablets.put(eachTabletInfo.getTabletName(), eachTabletInfo);
        }
      } catch (Exception e) {
        LOG.fatal(e);
        shutdown();
      }

      // scan root tablet finding all meta tablet list
      try {
        scanMetaTabletFromRoot();
      } catch (Exception e) {
        LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
        shutdown();
        return;
      }

      // assignment meta tablet
      try {
        assignMetaTablet(assignedTablets);
      } catch (IOException e) {
        LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
        shutdown();
        return;
      }

      // MetaTablet이 모두 할당되기를 기다린다.
      while (true) {
        synchronized (tabletInfoMonitor) {
          if (assigningTablets.isEmpty() && unassignedTablets.isEmpty()) {
            break;
          }
        }
        try {
          Thread.sleep(1 * 1000);
        } catch (InterruptedException e) {
          return;
        }
      }

      clusterReady = true;

      // 할당되지 않은 Tablet을 찾는다.
      initUnassignedUserTablet(assignedTablets);

      // 할당되지 않은 Tablet 할당
      initialTabletAssignCount = unassignedTablets.size();

      assignUserTablet();

      // Drop 미처리된 내역을 조회한다.
      completeDropedTable();

      // Split 도중 정리가 안된 Tablet에 대한 작업을 수행한다.
      finalizeSplitTemp();

      LOG.debug("End RootTabletAssignmentThread, TabletAssign thread start");
      Thread assignThread = new Thread(threadGroup, new Runnable() {
        public void run() {
          while (true) {
            try {
              Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
              return;
            }
            // LOG.debug("Assign Thread:unassignedTablets.size()=" +
            // unassignedTablets.size());
            assignUserTablet();
          }
        }
      });

      assignThread.start();
    }

    private void completeDropedTable() {
      // LOG.debug("Check drop while master init");
      try {
        String dropLockPath = Constants.TABLE_DROP;

        List<String> dropTableNames = null;
        try {
          dropTableNames = zk.getChildren(LockUtil.getZKPath(conf, dropLockPath), false);
        } catch(NoNodeException e) {
        }
        if (dropTableNames == null || dropTableNames.size() == 0) {
          return;
        }

        // 이미 존재하는 경우는 CloudataMaster가 장애 발생으로 재 시작된 경우
        // 이 경우에는 drop 완료된 tablet에 대한 정보를 확인하여 전체 tablet 중에 drop 완료된 tablet 목록은
        // 지운다.
        for (String eachTable: dropTableNames) {
          asyncTaskManager.runAsyncTask(new TableDropTask(eachTable));
        }

      } catch (Exception e) {
        LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
        shutdown();
      }
    }

    private void assignUserTablet() {
      Map<String, TabletInfo> assignTargetTablets = new HashMap<String, TabletInfo>(
          10);

      synchronized (tabletInfoMonitor) {
        assignTargetTablets.putAll(unassignedTablets);
      }

      for (TabletInfo eachTabletInfo : assignTargetTablets.values()) {
        try {
          assignTablet(eachTabletInfo);
        } catch (IOException e) {
          // e.printStackTrace();
        }
      }
    }

    private void initUnassignedUserTablet(
        Map<String, TabletInfo> assignedTablets) {
      List<TabletInfo> tempMetaTablets = new ArrayList<TabletInfo>();

      synchronized (tabletInfoMonitor) {
        Map<String, TabletInfo> metaTablets = tabletInfos.get(TABLE_NAME_META);
        tempMetaTablets.addAll(metaTablets.values());
      }

      for (TabletInfo metaTabletInfo : tempMetaTablets) {
        TableScanner scanner = null;
        try {
          try {
            scanner = ScannerFactory.openScanner(conf, metaTabletInfo, META_COLUMN_NAME_TABLETINFO);
          } catch (Exception e) {
            LOG.fatal("Can't META Scanner", e);
            shutdown();
          }

          Row scanRow = null;
          
          while ((scanRow = scanner.nextRow()) != null) {
            TabletInfo userTabletInfo = new TabletInfo();
            List<Cell> cells = scanRow.getCellList(META_COLUMN_NAME_TABLETINFO);
            
            if(cells.size() > 1) {
              for(Cell eachCell: cells) {
                userTabletInfo.readFields(eachCell.getBytes());
                LOG.fatal("Too many cells in META:" + scanRow.getKey() + "," + eachCell.getKey()); 
              }
              System.exit(0);
            }
            try {
              userTabletInfo.readFields(cells.get(cells.size() - 1).getBytes());
            } catch (EOFException e) {
              DataInputStream din = new DataInputStream(new ByteArrayInputStream(cells.get(cells.size() - 1).getBytes()));
              userTabletInfo.readOldFields(din);
            }
            
            String userTableName = userTabletInfo.getTableName();
            if(TableSchema.loadTableSchema(conf, zk, userTableName) == null) {
              continue;
            }
            Map<String, TabletInfo> tablets = tabletInfos.get(userTableName);
            if (tablets == null) {
              tablets = new HashMap<String, TabletInfo>();
              tabletInfos.put(userTableName, tablets);
            }
            tablets.put(userTabletInfo.getTabletName(), userTabletInfo);

            if (!assignedTablets.containsKey(userTabletInfo.getTabletName())) {
              synchronized (tabletInfoMonitor) {
                unassignedTablets.put(userTabletInfo.getTabletName(),
                    userTabletInfo);
              }
            }
          }
        } catch (Exception e) {
          LOG.fatal("initUnassignedUserTablet: " + metaTabletInfo, e);
          shutdown();
        } finally {
          if (scanner != null) {
            try {
              scanner.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }
    }

    /**
     * ROOT tablet을 scan하여 META tablet의 목록을 가져온다. tabletInfos 멤버변수에 추가
     * 
     * @throws IOException
     */
    private void scanMetaTabletFromRoot() throws IOException {
      // LOG.debug("Start scanMetaTabletFromRoot: ");
      Map<String, TabletInfo> metaTabletInfos = tabletInfos
          .get(TABLE_NAME_META);
      if (metaTabletInfos == null) {
        metaTabletInfos = new HashMap<String, TabletInfo>(100);
        tabletInfos.put(TABLE_NAME_META, metaTabletInfos);
      }

      TabletInfo rootTabletInfo = tabletInfos.get(TABLE_NAME_ROOT).values()
          .iterator().next();
      // LOG.debug("scanMetaTabletFromRoot: " + rootTabletInfo);

      // Root Tablet이 최초 생성된 후에는 Meta Tablet이 없다.
      TableScanner scanner = ScannerFactory.openScanner(conf,
          rootTabletInfo, META_COLUMN_NAME_TABLETINFO);
      try {
        ScanCell scanCell = null;
        while ((scanCell = scanner.next()) != null) {
          TabletInfo metaTabletInfo = new TabletInfo();
          metaTabletInfo.readFields(scanCell.getBytes());

          metaTabletInfos.put(metaTabletInfo.getTabletName(), metaTabletInfo);
        }
      } finally {
        scanner.close();
      }
    }
  }// end of RootTabletAssignmentThread class

  /**
   * 일정시간동안 TabletServer로부터 Heartbeat 메세지가 오지 않으면 TabletServer가 disable된 것으로
   * 판단하여 해당 TabletServer에서 서비스 중인 Tablet을 다른 TabletServer에 할당한다. 이 Thread에서는 실제
   * 할당하는 것까지 수행하지는 않고 unassignedTablet에 추가만 한다.
   * 
   * @author babokim
   */
  class ReassignmentWhenTabletServerFailThread implements Runnable {
    // FIXME TabletServer fail 처리 완료되지 않은 상태에서 똑같은 서버가 다시 살아나거나
    // 살아난 후 다시 fail 되는 경우에 대한 처리
    private TabletServerInfo tabletServerInfo;

    public ReassignmentWhenTabletServerFailThread(
        TabletServerInfo tabletServerInfo) {
      this.tabletServerInfo = tabletServerInfo;
    }

    public void run() {
      String failedHostName = tabletServerInfo.getHostName();

      LOG.debug("Failed TabletServer:" + failedHostName + ", time="
          + System.currentTimeMillis() + ", Master table count="
          + tabletInfos.size());

      // Split 처리중 META는 수정 전/후에 TabletServer가 fail 된 경우 CloudataMaster도 자동으로
      // 죽는다. 이유는 Tablet 정보가 불일치 하기 때문
      String splitLockPath = Constants.TABLETSERVER_SPLIT + "/" + failedHostName;
      List<String> splitedTablets = null;
      try {
        splitedTablets = zk.getChildren(LockUtil.getZKPath(conf, splitLockPath), false);
      } catch(NoNodeException e) {
      } catch (Exception e) {
        LOG.fatal("CloudataeMaster shutdown cause: can't drop info", e);
        shutdown();
        return;
      }
          
      if (splitedTablets != null && splitedTablets.size() > 0) {
        LOG.fatal("CloudataMaster shutdown cause: tablet server failed while spliting");
        shutdown();
        return;
      }
      
      // FIXME tabletInfos가 많은 경우 synchronized 블럭이 너무 오래 수행된다.
      TabletInfo rootTablet = null;
      Set<TabletInfo> metaTabletInfos = new HashSet<TabletInfo>();
      Set<TabletInfo> userTabletInfos = new HashSet<TabletInfo>();

      synchronized (tabletInfoMonitor) {
        // Master server에서 관리 중인 tablet 목록에서 fail된 서버의 tablet server에서 관리하는
        // tablet을 찾는다.
        for (Map<String, TabletInfo> tablets : tabletInfos.values()) {
          for (TabletInfo tabletInfo : tablets.values()) {
            if (failedHostName.equals(tabletInfo.getAssignedHostName())) {
              if (TABLE_NAME_ROOT.equals(tabletInfo.getTableName())) {
                rootTablet = tabletInfo;
                // LOG.debug("root Tablet on the failed TabletServer(tabletInfos):"
                // + tabletInfo);
              } else if (TABLE_NAME_META.equals(tabletInfo.getTableName())) {
                metaTabletInfos.add(tabletInfo);
                // LOG.debug("meta Tablet on the failed TabletServer(tabletInfos):"
                // + tabletInfo);
              } else {
                userTabletInfos.add(tabletInfo);
                // LOG.debug("user Tablet on the failed TabletServer(tabletInfos):"
                // + tabletInfo);
              }
              LOG.debug("Tablet on the failed TabletServer(tabletInfos):"
                  + tabletInfo);
              assigningTablets.remove(tabletInfo.getTabletName());
            }
          }
        }

        // 현재 할당 중인 tablet 목록에서 fail된 서버의 tablet server에서 관리하는 tablet을 찾는다.
        for (TabletInfo tabletInfo : assigningTablets.values()) {
          if (failedHostName.equals(tabletInfo.getAssignedHostName())) {
            if (TABLE_NAME_ROOT.equals(tabletInfo.getTableName())) {
              rootTablet = tabletInfo;
              // LOG.debug("root Tablet on the failed TabletServer(assigningTablets):"
              // + tabletInfo);
            } else if (TABLE_NAME_META.equals(tabletInfo.getTableName())) {
              metaTabletInfos.add(tabletInfo);
              // LOG.debug("meta Tablet on the failed TabletServer(assigningTablets):"
              // + tabletInfo);
            } else {
              userTabletInfos.add(tabletInfo);
              // LOG.debug("user Tablet on the failed TabletServer(assigningTablets):"
              // + tabletInfo);
            }
            // assigningTablets.remove(tabletInfo.getTabletName());
            LOG.debug("Tablet on the failed TabletServer(assigningTablets):"
                + tabletInfo);
          }
        }

        // 현재 drop 처리 중인 테이블의 Tablet의 할당 목록에서 삭제한다.
        List<TabletInfo> removeTargets = new ArrayList<TabletInfo>();
        for (TabletInfo tabletInfo : assigningTablets.values()) {
          if (dropingTabletServers.containsKey(tabletInfo.getTableName())) {
            removeTargets.add(tabletInfo);
          }
        }
        for (TabletInfo tabletInfo : removeTargets) {
          assigningTablets.remove(tabletInfo.getTabletName());
        }

        removeTargets = new ArrayList<TabletInfo>();
        for (TabletInfo tabletInfo : userTabletInfos) {
          if (dropingTabletServers.containsKey(tabletInfo.getTableName())) {
            removeTargets.add(tabletInfo);
          }
        }
        for (TabletInfo tabletInfo : removeTargets) {
          userTabletInfos.remove(tabletInfo.getTabletName());
        }
      }

      // ROOT Tablet 재할당
      if (rootTablet != null) {
        try {
          assignTablet(rootTablet);
        } catch (Exception e) {
          LOG.error("ReassignmentWhenTabletServerFailThread: assign ROOT");
        }
      }

      try {
        // //////////////////////////////////////////////////////
        // META Tablet 재할당
        for (TabletInfo tabletInfo : metaTabletInfos) {
          try {
            assignTablet(tabletInfo);
          } catch (Exception e) {
            LOG.error("ReassignmentWhenTabletServerFailThread: assign META");
          }
        }

        // //////////////////////////////////////////////////////
        // User Tablet 재할당
        for (TabletInfo tabletInfo : userTabletInfos) {
          try {
            assignTablet(tabletInfo);
          } catch (Exception e) {
            LOG
                .error("ReassignmentWhenTabletServerFailThread: assign userTabletInfos: "
                    + tabletInfo);
          }
        }
      } catch (Exception e) {
        LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
        shutdown();
      }
    }
  }

  /**
   * TabletServer에서 Split 도중 마지막 단계인 Master로 report하기 전/후로 장애가 발생한 경우에 대한 처리
   * Split은 정상적으로 되었기 때문에 이전에 있던 tablet의 파일과 정보를 삭제하는 기능을 수행한다.
   * 
   * @throws IOException
   */
  private void finalizeSplitTemp() {
    List<String> splitHostInfos = null;
    try {
      splitHostInfos = zk.getChildren(LockUtil.getZKPath(conf, Constants.TABLETSERVER_SPLIT), false);
    } catch(NoNodeException e) {
    } catch (Exception e) {
      LOG.warn("finalizeSplitTemp lockService.readDir error", e);
    }

    if (splitHostInfos == null || splitHostInfos.isEmpty()) {
      return;
    }

    try {
      for (String eachHostName : splitHostInfos) {
        LOG.info("finalizeSplitTemp:host=" + eachHostName);

        List<String> tabletSplitInfos = null;
        try {
          tabletSplitInfos = zk.getChildren(LockUtil.getZKPath(conf, Constants.TABLETSERVER_SPLIT + "/" + eachHostName), false);
        } catch(NoNodeException e) {
        }
        
        if (tabletSplitInfos == null || tabletSplitInfos.isEmpty()) {
          LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.TABLETSERVER_SPLIT + "/" + eachHostName), true);
          continue;
        }

        for (String splitTabletName: tabletSplitInfos) {
          LOG.info("finalizeSplitTemp:tablet=" + splitTabletName);
          String path = LockUtil.getZKPath(conf, Constants.TABLETSERVER_SPLIT + "/" + eachHostName + "/" + splitTabletName);
          byte[] splitInfo = zk.getData(path, false, null);
          DataInputStream din = new DataInputStream(new ByteArrayInputStream(splitInfo));
          String tableName = CWritableUtils.readString(din);
          
          TabletInfo[] splitTabletInfos = new TabletInfo[2];
          splitTabletInfos[0] = new TabletInfo();
          splitTabletInfos[0].readFields(din);
          splitTabletInfos[1] = new TabletInfo();
          splitTabletInfos[1].readFields(din);
          
          // 접속을 해서 해당 Tablet이 존재하는지 확인해본다.
          try {
            TabletInfo tabletInfo = connectTabletServer(hostName).getTabletInfo(splitTabletName);
            if (tabletInfo != null) {
              LOG.fatal("Tablet Split aborted abnormally." + splitTabletName
                  + "," + splitTabletInfos[0] + "," + splitTabletInfos[1]);
              //shutdown();
            }
          } catch (Exception e) {
            // 장애가 발생한 경우나 Tablet 정보가 존재하지 않는 경우
            LOG.error("finalizeSplitTemp:getTabletInfo Error:" + splitTabletName, e);
          }

          Map<String, TabletInfo> managedTabletInfos = tabletInfos
              .get(tableName);
          if (managedTabletInfos != null
              && managedTabletInfos.containsKey(splitTabletName)) {
            LOG.fatal("Tablet Split aborted abnormally." + splitTabletName
                + "," + splitTabletInfos[0] + "," + splitTabletInfos[1]);
            shutdown();
          } else {
            // 해당 Tablet의 모든 파일 삭제
            GPath dataPath = Tablet.getTabletPath(conf, tableName,
                splitTabletName);
            LOG.info("delete tablet files while finalize split:"
                + splitTabletName + ":" + dataPath);
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
                LOG.warn("Can't delete " + dataPath + " while droping "
                    + splitTabletName);
                break;
              }
            }
          }
        } // for each tablet
        LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.TABLETSERVER_SPLIT + "/" + eachHostName), true);
      } // for each tabletserver
    } catch (Exception e) {
      LOG.fatal("CloudataMaster shutdown cause:" + e.getMessage(), e);
      shutdown();
    }
  }

  private void addRunningTabletServer(String tabletServerHostName)
      throws IOException {
    LOG.info("Notice tablet server created: " + tabletServerHostName);

    TabletServerInfo tabletServerInfo = new TabletServerInfo(
        tabletServerHostName);
    if (liveTabletServers.containsKey(tabletServerHostName)) {
      return;
    }
    // call release all tablets. if tablet server reconnected.
    // connectTabletServer(tabletServerInfo).stopAllTablets();
    liveTabletServers.put(tabletServerHostName, tabletServerInfo);
    
    int ratio = conf.getInt("cloudatamaster.iolock.ratio", 1);
    
    synchronized(Constants.SC_LOCK_PATH) {
      for(int i = 0; i < ratio; i++) {
        addDiskIOLock(Constants.SC_LOCK_PATH, Constants.SC_LOCK_NAME, scLockNo);
        addDiskIOLock(Constants.MC_LOCK_PATH, Constants.MC_LOCK_NAME, mcLockNo);
      }
    }
    
    if(deadTabletServers.contains(tabletServerHostName)) {
      deadTabletServers.remove(tabletServerHostName);
    }
  }

  class MasterNodeDeletedWatcher implements Watcher {
    String masterLockPathName;
    MasterNodeDeletedWatcher(String masterLockPathName) {
      this.masterLockPathName = masterLockPathName;
    }
    
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.None) {
        switch (event.getState()) {
        case SyncConnected:
          break;
        case Disconnected:
        case Expired:
          LOG.info("Shutdown cause lock expired:" + event);
          shutdown();
          break;
        }
      } else if(event.getType() == Event.EventType.NodeDeleted) {
        synchronized(masterElectMonitor) {
          masterElectMonitor.notifyAll();
        }
      }
    }
  }
  
  class TabletServerWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if(event.getType() == Event.EventType.NodeChildrenChanged) {
        try {
          List<String> tabletServerLocks = null;
          try {
            tabletServerLocks = zk.getChildren(LockUtil.getZKPath(conf, Constants.SERVER), this);
          } catch(NoNodeException e) {
          }
          
          if(tabletServerLocks == null) {
            tabletServerLocks = new ArrayList<String>();
          }
          
          synchronized(liveTabletServers) {
            Set<String> tmpTabletServers = new HashSet<String>();
            tmpTabletServers.addAll(liveTabletServers.keySet());
            for(String eachTabletServer: tabletServerLocks) {
              
              if(!tmpTabletServers.contains(eachTabletServer)) {
                addRunningTabletServer(eachTabletServer);
              } else {
                tmpTabletServers.remove(eachTabletServer);
              }
            }
            
            for(String failedTabletServer: tmpTabletServers) {
              processTabletServerFail(failedTabletServer);
              liveTabletServers.remove(failedTabletServer);
            }
            liveTabletServers.notifyAll();
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }
  
  class CommitLogServerWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if(event.getType() == Event.EventType.NodeChildrenChanged) {
        try {
          List<String> commitLogServerLocks = null;
          try {
            commitLogServerLocks = zk.getChildren(LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), this);
          } catch(NoNodeException e) {
          }
          
          if(commitLogServerLocks == null) {
            commitLogServerLocks = new ArrayList<String>();
          }
          
          synchronized(liveCommitLogServers) {
            Set<String> tmpLiveCommitLogServers = new HashSet<String>();
            tmpLiveCommitLogServers.addAll(liveCommitLogServers.keySet());
            
            for(String eachCommitLogServer: commitLogServerLocks) {
              if(tmpLiveCommitLogServers.contains(eachCommitLogServer)) {
                tmpLiveCommitLogServers.remove(eachCommitLogServer);
              } else {
                liveCommitLogServers.put(eachCommitLogServer, new ServerMonitorInfo());
                deadCommitLogServers.remove(eachCommitLogServer);
              }
            }
            
            for(String failedCommitLogServer: tmpLiveCommitLogServers) {
              deadCommitLogServers.add(failedCommitLogServer);
              liveCommitLogServers.remove(failedCommitLogServer);
            }
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }
  
  private void processTabletServerFail(String tabletServerHostName) {
    TabletServerInfo tabletServerInfo = new TabletServerInfo(tabletServerHostName);

    if (!liveTabletServers.containsKey(tabletServerHostName)) {
      return;
    }
    LOG.info("Notice tablet server failed: " + tabletServerHostName);
    liveTabletServers.remove(tabletServerHostName);

    Thread thread = new Thread(threadGroup,
        new ReassignmentWhenTabletServerFailThread(tabletServerInfo));
    thread.start();

    // Drop 처리 중인 TabletServer의 목록에서 삭제
    synchronized (dropingTabletServers) {
      for (List<String> entry : dropingTabletServers.values()) {
        entry.remove(tabletServerHostName);
      }
    }
    
    deadTabletServers.add(tabletServerHostName);
  }
  
  private void addLockEventHandler() throws IOException {
    //TabletServer가 추가되었을 경우
    synchronized(liveTabletServers) {
      try {
        if(zk.exists(LockUtil.getZKPath(conf, Constants.SERVER), false) == null) {
          LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.SERVER), null, CreateMode.PERSISTENT);
        }
      } catch (NodeExistsException e) {
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      List<String> servers;
      try {
        LOG.info("TabletServer lock watcher registered.");
        servers = zk.getChildren(LockUtil.getZKPath(conf, Constants.SERVER), new TabletServerWatcher());
      } catch (Exception e) {
        throw new IOException(e);
      }
  
      if (servers != null) {
        for (String eachServer: servers) {
          addRunningTabletServer(eachServer);
        }
        liveTabletServers.notifyAll();
      } else {
        LOG.info("No lock in " + Constants.SERVER);
      }
    }
    
    synchronized(liveCommitLogServers) {
      try {
        if(zk.exists(LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), false) == null) {
          LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), null, CreateMode.PERSISTENT);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      List<String> servers;
      try {
        LOG.info("CommitLogServer lock watcher registered.");
        servers = zk.getChildren(LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), new CommitLogServerWatcher());
      } catch (Exception e) {
        throw new IOException(e);
      }
  
      if (servers != null) {
        for (String eachServer: servers) {
          liveCommitLogServers.put(eachServer, new ServerMonitorInfo());
        }
      }
    }
  }
  
  public String getHostName() {
    return this.hostName;
  }

  public String getTestHandlerKey() {
    return hostName;
  }

  public void test() {
    LOG.debug("called test:" + hostName);
  }

  public CloudataConf getConf() {
    return conf;
  }

  public TabletServerInfo[] getTabletServerInfos() {
    synchronized (liveTabletServers) {
      if (liveTabletServers.isEmpty()) {
        return null;
      }
      return liveTabletServers.values().toArray(new TabletServerInfo[] {});
    }
  }

  public void addUser(String userId) throws IOException {
    // 여기에 전달되는 passwd는 평문
    if (!AclManager.isSuperGroup(conf, zk)) {
      throw new IOException("Can't access user management API");
    }
    addUser(conf, zk, userId);
  }

  private static void addUser(CloudataConf conf, ZooKeeper zk, String userId)
      throws IOException {
    LockUtil.createNodes(zk, LockUtil.getZKPath(conf, USERS + "/" + userId), userId.getBytes(), CreateMode.PERSISTENT);
  }

  private static void addSuperGroupUser(CloudataConf conf, ZooKeeper zk, String userId)
      throws IOException {
    LockUtil.createNodes(zk, LockUtil.getZKPath(conf, SUPERGROUP), userId.getBytes(), CreateMode.PERSISTENT);
  }

  private boolean existSuperGroup() throws IOException {
    try {
      return zk.exists(LockUtil.getZKPath(conf, SUPERGROUP), false) != null;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void removeUser(String userId) throws IOException {
    if (!AclManager.isSuperGroup(conf, zk)) {
      throw new IOException("Can't access user management API");
    }

    try {
      LockUtil.delete(zk, LockUtil.getZKPath(conf, USERS + "/" + userId), true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void addTablePermission(String tableName, String userId,
      String readWrite) throws IOException {
    AclManager.checkOwner(conf, zk, schemaMap, tableName);

    TableSchema tableSchema = schemaMap.get(tableName);

    if (tableSchema == null) {
      throw new IOException("No table:" + tableName);
    }

    tableSchema.addPermission(userId, readWrite);
    tableSchema.saveTableSchema(conf, zk);
  }

  public void removeTablePermission(String tableName, String userId)
      throws IOException {
    AclManager.checkOwner(conf, zk, schemaMap, tableName);

    TableSchema tableSchema = schemaMap.get(tableName);
    
    if (tableSchema == null) {
      throw new IOException("No table:" + tableName);
    }

    tableSchema.removePermission(userId);
    tableSchema.saveTableSchema(conf, zk);
  }

  protected Set<String> getDeadTabletServers() {
    return this.deadTabletServers;
  }
  
  protected Map<String, ServerMonitorInfo> getLiveCommitLogServers() {
    return this.liveCommitLogServers;
  }
  
  protected Set<String> getDeadCommitLogServers() {
    return this.deadCommitLogServers;
  }
  
  //Table Name -> # Tablet(total, unassign, assigning)
  protected Map<String, Integer[]> getNumTablets() {
    Map<String, Integer[]> result = new HashMap<String, Integer[]>();

    synchronized(tabletInfos) {
      for(Map.Entry<String, Map<String, TabletInfo>> entry: tabletInfos.entrySet()) {
        String tableName = entry.getKey();
        Map<String, TabletInfo> tableTablets = entry.getValue();
        result.put(tableName, new Integer[]{tableTablets.keySet().size(), 0, 0}); 
      }
    }
    
    synchronized(tabletInfoMonitor) {
      for(TabletInfo eachTableInfo: unassignedTablets.values()) {
        String tableName = eachTableInfo.getTableName();
        Integer[] nums = result.get(tableName);
        if(nums == null) {
          result.put(tableName, new Integer[]{0, 1, 0}); 
        } else {
          nums[1]++;
          //result.put(tableName, nums); 
        }
      }
      
      for(TabletInfo eachTableInfo: assigningTablets.values()) {
        String tableName = eachTableInfo.getTableName();
        Integer[] nums = result.get(tableName);
        if(nums == null) {
          result.put(tableName, new Integer[]{0, 0, 1}); 
        } else {
          nums[2]++;
          //result.put(tableName, nums); 
        }
      }   
    }
    
    return result;
  }
  
  public void heartbeatTS(String hostName, int tabletNum) throws IOException {
    TabletServerInfo info = liveTabletServers.get(hostName);
    if(info != null) {
      info.setLastHeartbeatTime(System.currentTimeMillis());
      if (initialTabletEndCount > initialTabletAssignCount * 0.8) {
        info.setNumOfTablets(tabletNum);
      }
    }
  }
  
  public void heartbeatCS(String hostName, ServerMonitorInfo serverMonitorInfo) throws IOException {
    if(liveCommitLogServers.containsKey(hostName)) {
      serverMonitorInfo.setLastHeartbeatTime(System.currentTimeMillis());
      liveCommitLogServers.put(hostName, serverMonitorInfo);
    }
  }

  /**
   * @return
   */
  public String getLockStatus() throws IOException {
    List<String> scLocks = null;
    try {
      scLocks = zk.getChildren(LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH), false);
    } catch(NoNodeException e) {
      return "Split Lock: 0/0 (current/total)";
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (scLocks == null || scLocks.size() == 0) {
      return "Split Lock: 0/0 (current/total)";
    }
    
    int total = scLocks.size();
    int locked = 0;
    for(String path : scLocks) {
      try {
        List<String> children = zk.getChildren(LockUtil.getZKPath(conf, Constants.SC_LOCK_PATH + "/" + path), false);
        if(children.size() > 0) {
          locked++;
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    
    return "Split Lock: " + locked + "/" + total + " (current/total)";
  }
  
  public synchronized void startBalancer() throws IOException {
    if(balancer == null) {
      balancer = new Balancer();
      Thread balancerThread = new Thread(threadGroup, balancer);
      balancerThread.start();
    } else {
      LOG.info("Balancer already started");
      return;
    }
  }
  
  class Balancer implements Runnable {
    static final float BALANCE_RATE = 0.9f;
    
    List<TabletServerInfo> idleTabletServers = new ArrayList<TabletServerInfo>();
    Map<String, TabletServerInfo> targetTabletServers = new HashMap<String, TabletServerInfo>();
    List<TabletInfo> targetTablets = new ArrayList<TabletInfo>();
    
    public void run() {
      try {
        LOG.info("Balancer.start");
        
        List<TabletServerInfo> tabletServers = new ArrayList<TabletServerInfo>();
        synchronized (liveTabletServers) {
          if (liveTabletServers.isEmpty()) {
            return;
          }
          tabletServers.addAll(liveTabletServers.values());
        }
        
        int totalTablets = 0;
        for(TabletServerInfo eachTabletServer: tabletServers) {
          totalTablets += eachTabletServer.getNumOfTablets();
        }
        
        int avgTablet = totalTablets/tabletServers.size();
        
        int balancingTarget = 0;
        
        for(TabletServerInfo eachTabletServer: tabletServers) {
          if((float)eachTabletServer.getNumOfTablets() * BALANCE_RATE < avgTablet) {
            balancingTarget += (avgTablet - eachTabletServer.getNumOfTablets() * BALANCE_RATE);
            idleTabletServers.add(eachTabletServer);
          } else {
            targetTabletServers.put(eachTabletServer.getHostName(), eachTabletServer);
          }
        }
        
        if(idleTabletServers.size() == 0 || targetTabletServers.size() == 0) {
          LOG.info("Balancer.stop cause " + idleTabletServers.size() + "," + targetTabletServers.size());
          return;
        }
        
        int targetTabletNumPerTabletServer = balancingTarget/targetTabletServers.size();
      
        if(targetTabletNumPerTabletServer > 50) {
          targetTabletNumPerTabletServer = 50;
        }
        
        synchronized(idleTabletServers) {
          for(TabletServerInfo eachTabletServer: targetTabletServers.values()) {
            try {
              TabletManagerProtocol tabletServer = CloudataMaster.this.connectTabletServer(eachTabletServer);
              LOG.info("Balancer.call electRebalacingTablets to " + eachTabletServer.getHostName() + "," + targetTabletNumPerTabletServer);
              tabletServer.electRebalacingTablets(targetTabletNumPerTabletServer);
            } catch (IOException e) {
              LOG.warn("Tablet Balancing Error:" + eachTabletServer.getHostName() + "," + e.getMessage(), e);
            }
          }

          while(true) {
            try {
              idleTabletServers.wait();
            } catch (InterruptedException e) {
            }
            
            List<TabletInfo> tmpTargetTablets = new ArrayList<TabletInfo>(); 
            synchronized(targetTablets) {
              tmpTargetTablets.addAll(targetTablets);
              targetTablets.clear();
            }
            for(TabletInfo tabletInfo: tmpTargetTablets) {
              try {
                assignTablet(tabletInfo, idleTabletServers);
              } catch (IOException e) {
                LOG.error("Can't assign while rebalnacing: " + e.getMessage(), e);
              }
            }
            
            if(targetTabletServers.size() == 0) {
              break;
            }
          }
        }
      } finally {
        LOG.info("Balancer.end");
        balancer = null;
      }
    }
    
    public void addTargetTablets(String tabletServerHostName, TabletInfo tabletInfo, boolean end) {
      synchronized(idleTabletServers) {
        if(end) {
          targetTabletServers.remove(tabletServerHostName);
        }
        if(tabletInfo != null) {
	 /*
          synchronized (tabletInfoMonitor) {
            TabletServerInfo tabletServerInfo = liveTabletServers.get(tabletServerHostName);
            if (tabletServerInfo != null) {
              tabletServerInfo.subtractNumOfTablets();
            }
          }
	 */
          synchronized(targetTablets) {
            targetTablets.add(tabletInfo);
          }
        }
        idleTabletServers.notify();
      }
    }
  }
  
  public void doRebalacing(String tabletServerHostName, TabletInfo tabletInfo, boolean end) throws IOException {
    LOG.info("Balancer.received balancing tablet from " + tabletServerHostName + "," + tabletInfo.getTabletName());
    balancer.addTargetTablets(tabletServerHostName, tabletInfo, end);
  }
  
  class UpdateMetricsThread implements Runnable {
    
    public void run() {
      while(true) {
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
          return;
        }
        masterMetrics.setLiveTabletServerNum(liveTabletServers.size());
        masterMetrics.setDeadTabletServerNum(deadTabletServers.size());
        masterMetrics.setLiveCommitLogServerNum(liveCommitLogServers.size());
        masterMetrics.setDeadCommitLogServerNum(deadCommitLogServers.size());
        
        int numTablets = 0;
        synchronized(tabletInfos) {
          for(Map.Entry<String, Map<String, TabletInfo>> entry: tabletInfos.entrySet()) {
            numTablets += entry.getValue().keySet().size();
          }
        }
        
        masterMetrics.setTotalTabletNum(numTablets);
      }
    }
  }
}

