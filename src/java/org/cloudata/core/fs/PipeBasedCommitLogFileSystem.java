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
package org.cloudata.core.fs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.commitlog.CommitLogClient;
import org.cloudata.core.commitlog.CommitLogInterruptedException;
import org.cloudata.core.commitlog.CommitLogOverLoadedException;
import org.cloudata.core.commitlog.CommitLogStatus;
import org.cloudata.core.commitlog.ServerLocationManager;
import org.cloudata.core.commitlog.ServerSet;
import org.cloudata.core.commitlog.UnmatchedLogException;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.CommitLogException;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.tabletserver.CommitLog;
import org.cloudata.core.tabletserver.TabletServer;


/**
 * Commitlog 처리를 위한 파일시스템 클라이언트 현재 구성은 Cloudata에서 자체 개발된 고속의 append 가능한 commit
 * log용 파일시스템을 별도로 구축하여 사용하고 있다.
 * 
 * @author babokim
 * 
 */
public class PipeBasedCommitLogFileSystem implements CommitLogFileSystemIF {
  static final Log LOG = LogFactory.getLog(PipeBasedCommitLogFileSystem.class);

  private static final int MAX_NUM_TRY = 3;

  static final ConcurrentHashMap<ServerSet, CommitLogFileSystemInfo> fsCache = new ConcurrentHashMap<ServerSet, CommitLogFileSystemInfo>();

  public static final AtomicInteger totalPipeCount = new AtomicInteger(0);

  static final Thread pipeCullerThread;

  static {
    pipeCullerThread = createPipeCullerThread();
    pipeCullerThread.start();
  }

  static final Set<String> staleTabletNameSet = Collections
      .synchronizedSet(new TreeSet<String>());

  // CommitLogFileSystemInfo의 모든 public 메소드들은
  // 호출하기 전에 쓰레드가 fsCache의 Lock을 획득해야 한다.
  static class CommitLogFileSystemInfo {
    LinkedList<CommitLogClient> clientList = new LinkedList<CommitLogClient>();

    CommitLogFileSystemInfo() {
    }

    public void add(CommitLogClient fs) {
      synchronized (clientList) {
        clientList.addFirst(fs);
      }
    }

    public CommitLogClient removeFirst() {
      CommitLogClient client = null;

      synchronized (clientList) {
        if ((client = clientList.pollFirst()) == null) {
          return null;
        }
      }

      return client;
    }

    public void clear() {
      synchronized (clientList) {
        for (CommitLogClient fs : clientList) {
          fs.makeExpired();
        }
      }
    }
  }

  static String lastReport = null;

  static long lastReportTime = System.currentTimeMillis();

  private static void report() {
    long t = System.currentTimeMillis();
    if ((t - lastReportTime) > 60000) {
      String msg = "fsCache size : " + fsCache.size();
      if (lastReport == null || lastReport.equals(msg) == false) {
        //LOG.info(msg);
        lastReport = msg;
      }

      lastReportTime = t;
    }
  }

  private static Thread createPipeCullerThread() {
    Thread pipeCullerThread = new Thread() {
      public void run() {
        LOG.info("Pipe culler thread starts");

        List<CommitLogClient> closeList = new ArrayList<CommitLogClient>();

        while (true) {
          report();

          try {
            Thread.sleep(2000);

            if(totalPipeCount.get() < 20) {
              continue;
            }
            extractExpiredClient(closeList);

            if (!closeList.isEmpty()) {
              for (final CommitLogClient fs : closeList) {
                try {
                  fs.close();
                } catch (Exception e) {
                  LOG.warn("Exception in closing fs but forced to close, ex : "
                      + e);
                } finally {
                  totalPipeCount.decrementAndGet();
                }
              }

              closeList.clear();
            }
          } catch (InterruptedException e) {
            LOG.info("Pipe culler thread ends");
            return;
          } catch (Exception e) {
            LOG.error("Exception in pipe culler thread, but continue", e);
          }
        }
      }

      private void extractExpiredClient(List<CommitLogClient> closeList) {
        Iterator<Map.Entry<ServerSet, CommitLogFileSystemInfo>> mapIter = fsCache
            .entrySet().iterator();

        while (mapIter.hasNext()) {
          Map.Entry<ServerSet, CommitLogFileSystemInfo> entry = mapIter.next();
          LinkedList<CommitLogClient> fsList = entry.getValue().clientList;

          synchronized (fsList) {
            Iterator<CommitLogClient> listIter = fsList.iterator();

            while (listIter.hasNext()) {
              CommitLogClient fs = listIter.next();

              if (fs.isExpired()) {
                if (fs.isClosed() == false) {
                  closeList.add(fs);
                }
                listIter.remove();
              }
            }
          }

          if (fsList.size() == 0) {
            mapIter.remove();
          }
        }
      }
    };

    pipeCullerThread.setDaemon(true);
    return pipeCullerThread;
  }

  CloudataConf conf;

  ServerLocationManager serverLocationManager;

  ThreadLocal<CommitLogClient> fsThreadLocal = new ThreadLocal<CommitLogClient>();

  boolean testmode = false;

  int maxPipePoolSize;

  ZooKeeper zk;

  PipeBasedCommitLogFileSystem(boolean consistencyCheck) {
  }

  public boolean ping(String tabletName) throws IOException {
    ServerSet serverSet = serverLocationManager.getTabletServerSet(tabletName);
    if (serverSet == null) {
      return false;
    }

    try {
      for (InetSocketAddress address : serverSet.getAddressList()) {
        String lockPath = Constants.COMMITLOG_SERVER + "/"
            + address.getHostName() + ":" + address.getPort();
        if (zk.exists(LockUtil.getZKPath(conf, lockPath), false) == null) {
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }

    return true;
  }

  public void open(String tabletName, boolean writable) throws IOException {
    if (staleTabletNameSet.contains(tabletName)) {
      LOG.info("tablet [" + tabletName + "] is already in staleTabletNameSet");
      throw new CommitLogInterruptedException();
    }

    CommitLogClient commitLogClient = getFromCache(tabletName);

    if (commitLogClient.isClosed() && writable) {
      if (totalPipeCount.incrementAndGet() >= this.maxPipePoolSize) {
        LOG.warn(tabletName + " pipe count(" + totalPipeCount.get() + ") exceeded maxPipePoolSize(" + maxPipePoolSize + ")");
        totalPipeCount.decrementAndGet();
        throw new CommitLogOverLoadedException();
      }

      try {
        commitLogClient.touch();
        
        commitLogClient.open();

        LOG.debug("Pipe for tablet : " + tabletName + " is open");
      } catch (CommitLogInterruptedException e) {
        returnToCache(tabletName, commitLogClient);
        throw e;
      } catch (UnmatchedLogException e) {
        returnToCache(tabletName, commitLogClient);
        throw new CommitLogException("Unmatched change logs, tabletName ["
            + tabletName + "]");
      } catch (IOException e) {
        fsThreadLocal.set(null);

        try {
          commitLogClient.close();
        } catch (IOException ex) {
          LOG.info("Error in closing pipe : " + ex);
        } finally {
          totalPipeCount.decrementAndGet();
        }
        throw e;
      }
    }

    if (writable) {
      fsThreadLocal.set(commitLogClient);
    } else {
//      dataList = null;
//      curLogIndex = 0;
//      curDataIndex = 0;

      returnToCache(tabletName, commitLogClient);
    }
  }

//  public void registerToStaleTabletNameSet(String tabletName) {
//    if (staleTabletNameSet.add(tabletName) == false) {
//      LOG.info("Tablet [" + tabletName + "] is already in stale set");
//    } else {
//      LOG.info("put tablet [" + tabletName + "] to stale set");
//    }
//  }

  private CommitLogClient getOpenedCommitLogClient(String tabletName)
      throws IOException {
    CommitLogClient fs = null;
    int numTry = 0;

    while ((fs = fsThreadLocal.get()) == null || fs.isClosed()) {
      if (++numTry >= MAX_NUM_TRY) {
        throw new IOException("unable to open commit log...");
      }

      LOG.info("reopen commit log client");
      open(tabletName, true);
      fs = fsThreadLocal.get();
    }

    return fs;
  }

  public boolean addCommitLog(String tabletName, String txId, int seq,
      CommitLog commitLog) throws IOException {
    CommitLogClient fs = null;

    if ((fs = fsThreadLocal.get()) == null || fs.isClosed()) {
      fs = getOpenedCommitLogClient(tabletName);
      fsThreadLocal.set(fs);
      LOG.info("newly set fs thread local 1");
    }

    if (!fs.isWritingStarted()) {
      try {
        fs.startWriting(seq, tabletName);
      } catch (SocketTimeoutException e) {
        returnToCache(tabletName, fs);
        throw e;
      } catch (CommitLogInterruptedException e) {
        returnToCache(tabletName, fs);
        throw e;
      }
    }

    fs.append(commitLog);

    return true;
  }

  public void processCommitLogServerFailed(Set<String> failedHostNames,
      List<String> tabletNames) {
    HashMap<ServerSet, CommitLogFileSystemInfo> tempFsCache = new HashMap<ServerSet, CommitLogFileSystemInfo>();
    tempFsCache.putAll(fsCache);

    for (Map.Entry<ServerSet, CommitLogFileSystemInfo> entry : tempFsCache
        .entrySet()) {
      ServerSet set = entry.getKey();
      InetSocketAddress[] addressList = set.getAddressList();

      for (InetSocketAddress eachAddress : addressList) {
        String hostName = eachAddress.getHostName() + ":"
            + eachAddress.getPort();
        if (failedHostNames.contains(hostName)) {
          CommitLogFileSystemInfo info = entry.getValue();
          info.clear();
        }
      }
    }
    for (String tabletName : tabletNames) {
      staleTabletNameSet.add(tabletName);
    }
  }

  public void finishAdding(String tabletName, String txId) throws IOException {
    CommitLogClient fs = null;

    if ((fs = fsThreadLocal.get()) == null || fs.isClosed()) {
      fs = getOpenedCommitLogClient(tabletName);
      fsThreadLocal.set(fs);
      LOG.info("newly set fs thread local 1");
    }

    try {
      fs.commitWriting();
      fs.touch();

      returnToCache(tabletName, fs);
    } catch (CommitLogInterruptedException e) {
      returnToCache(tabletName, fs);
      throw e;
    } finally {
      //watch.stopAndReportIfExceed(LOG);
      fsThreadLocal.set(null);
    }
  }

  void returnToCache(String tabletName, CommitLogClient fs) throws IOException {
    if (fs == null) {
      Thread.dumpStack();
    }

    ServerSet set = serverLocationManager.getReplicaAddresses(tabletName);
    CommitLogFileSystemInfo fsInfo = fsCache.get(set);

    if (fsInfo == null) {
      fsInfo = new CommitLogFileSystemInfo();
      CommitLogFileSystemInfo previous = fsCache.putIfAbsent(set, fsInfo);
      fsInfo = (previous == null) ? fsInfo : previous;
    }

    fsInfo.add(fs);
  }

  CommitLogClient getFromCache(String tabletName) throws IOException {
    CommitLogClient fs = null;
    ServerSet serverSet = serverLocationManager.getReplicaAddresses(tabletName);
    CommitLogFileSystemInfo fsInfo = fsCache.get(serverSet);

    if (fsInfo != null && (fs = fsInfo.removeFirst()) != null) {
      //return fs.touch();
      return fs;
    }

    return new CommitLogClient(conf, serverSet.getAddressList());
  }

  public void close(String tabletName, boolean writable) throws IOException {
    serverLocationManager.removeReplica(tabletName);
  }

  public void delete(String tabletName) throws IOException {
    CommitLogClient fs = new CommitLogClient(conf, serverLocationManager
        .getReplicaAddresses(tabletName).getAddressList());

    fs.removeAllLogs(tabletName);
  }

  public void startMinorCompaction(String tabletName) throws IOException {
    if (!staleTabletNameSet.contains(tabletName)) {
      CommitLogClient fs = new CommitLogClient(conf, serverLocationManager
          .getReplicaAddresses(tabletName).getAddressList());
      fs.mark(tabletName);
    }
  }

  public void endMinorCompaction(String tabletName) throws IOException {
    if (staleTabletNameSet.remove(tabletName)) {
      removeCommitLogServerInfo(tabletName);
    }
  }

  public boolean exists(String tabletName) throws IOException {
    CommitLogClient fs = new CommitLogClient(conf, serverLocationManager
        .getReplicaAddresses(tabletName).getAddressList());

    try {
      return fs.dirExists(tabletName) ? fs.getLogFileSize(tabletName) > 0
          : false;
    } catch (UnmatchedLogException e) {
      LOG.warn("Existence check of tablet[" + tabletName
          + "] returns TRUE even though exception occurs :" + e);
      // by sangchul
      // if the size of log files are different, existing check should return
      // true value
      return true;
    }
  }

  public void format() throws IOException {
    for (InetSocketAddress addr : serverLocationManager.getAllAddresses()) {
      CommitLogClient.formatLogsAt(conf, addr);
    }
  }

  public InetSocketAddress[] getCommitLogServerInfo(String tabletName)
      throws IOException {
    return serverLocationManager.getReplicaAddresses(tabletName)
        .getAddressList();
  }

  private void removeCommitLogServerInfo(String tabletName) throws IOException {
    serverLocationManager.removeReplica(tabletName);
  }

  public void init(CommitLogFileSystemIF systemIF, CloudataConf conf,
      TabletServer tabletServer, ZooKeeper zk) throws IOException {
    this.conf = conf;
    this.maxPipePoolSize = conf
        .getInt("commitLogClient.max.pipe.poolsize", 300); // 300
    this.testmode = Boolean.valueOf(conf.get("testmode", "false"));
    this.zk = zk;
    this.serverLocationManager = ServerLocationManager.instance(conf,
        tabletServer, zk, this);
    LOG.debug("PipeBasedCommitLogFileSystem is initialized. maxPipePoolSize ["
        + this.maxPipePoolSize + "]");
  }
  
  public CommitLogLoader getCommitLogLoader(String tabletName, CommitLogStatus commitLogStatus) throws IOException {
    return new CommitLogLoader(conf, tabletName, commitLogStatus, 
        serverLocationManager.getReplicaAddresses(tabletName).getAddressList());
  }
  
  public CommitLogStatus verifyCommitlog(String tabletName) throws IOException {
    CommitLogStatus status = new CommitLogStatus();

    CommitLogClient fs = new CommitLogClient(conf, serverLocationManager
        .getReplicaAddresses(tabletName).getAddressList());
    
    boolean isInconsistent = false;
    try {
      isInconsistent = fs.hasInvalidCommitLog(tabletName);
      status.setNeedCompaction(isInconsistent);

      if (isInconsistent) {
        LOG.warn("CommitLogs of tablet [" + tabletName + "] are inconsistent");
      }
    } catch (Exception e) {
      LOG.warn("Exception in verifying commit logs", e);
      status.setNeedCompaction(true);
    } finally {
      fs.close();
    }

    return status;
  }
}
