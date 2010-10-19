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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.commitlog.RandomStrategy.InetSocketAddressComparator;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.CommitLogException;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.fs.CommitLogFileSystemIF;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.DiskInfo;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tabletserver.TabletServer;


public class ServerLocationManager {
  static final Log LOG = LogFactory.getLog(ServerLocationManager.class);
  private static Object singletonLock = new Object();
  private static Map<String, ServerLocationManager> serverLocationManagers = new HashMap<String, ServerLocationManager>();

  public static ServerLocationManager instance(CloudataConf conf, TabletServer tabletServer, ZooKeeper zk, CommitLogFileSystemIF commitLogFs)
      throws IOException {
    synchronized (singletonLock) {
      String hostName = tabletServer == null ? "default" : tabletServer.getHostName();
      ServerLocationManager singleton = serverLocationManagers.get(hostName);
      if (singleton == null) {
        if (conf == null) {
          throw new IOException("NConfiguration instance is required "
              + "to initiate singleton instance of ServerLocationManager");
        }
        singleton = new ServerLocationManager(conf, tabletServer, zk, commitLogFs);
        serverLocationManagers.put(hostName, singleton);
      }
      singleton.loadFromZooKeeper();

      return singleton;
    }
  }

  private String logImagePath;
  private int numReplicas;

  ReentrantReadWriteLock listManagerLock = new ReentrantReadWriteLock();
  ServerListManagementStrategy listManager;

  ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
  //tabletName -> ServerSet
  Map<String, ServerSet> cache = new HashMap<String, ServerSet>();

  Set<String> runningServers = new HashSet<String>();
  
  AtomicBoolean hasAlreadyBeenLoaded = new AtomicBoolean(false);

  private CloudataFileSystem fs;
  private int pipePort;
  private ZooKeeper zk;
  private CloudataConf conf;
  private TabletServer tabletServer;
  private CommitLogFileSystemIF commitLogFs;
  private int maxDiskUsage;
  private CommitLogServerCapacityMonitor capacityMonitor;

  //hostName -> DiskInfo
  private Map<InetSocketAddress, DiskInfo> diskInfos = new HashMap<InetSocketAddress, DiskInfo>();
  
  private ServerLocationManager(CloudataConf conf, 
      TabletServer tabletServer, 
      ZooKeeper zk,
      CommitLogFileSystemIF commitLogFs) throws IOException {
    this.conf = conf;
    this.zk = zk;
    this.tabletServer = tabletServer;
    this.commitLogFs = commitLogFs;
    
    this.logImagePath = conf.get("commitlog.image.dir",
        "/user/cloudata/commitlog");
    
    this.maxDiskUsage = (int)(100f * conf.getFloat("commitlog.data.diskUsage", 80.0f));
    
    initStrategy(conf);
    hasAlreadyBeenLoaded.set(false);
    loadFromZooKeeper();
    initCloudataFileSystem(conf);
    
    capacityMonitor = new CommitLogServerCapacityMonitor();
    capacityMonitor.setDaemon(true);
    capacityMonitor.start();
  }

  private void initStrategy(CloudataConf conf) {
    String strategy = conf
        .get("commitLogServer.Selection.Strategy", "sequence");
    numReplicas = conf.getInt("commitlog.num.replicas", 2);
    pipePort = conf.getInt("cloudata.commitlog.server.port", 57001)
        + CommitLogServer.PORT_DIFF;

    if (strategy.equalsIgnoreCase("random")) {
      LOG.info("CommitLogServer selection strategy is RANDOM");
      listManager = new RandomStrategy(this, numReplicas, pipePort);
    } else if (strategy.equalsIgnoreCase("sequence")) {
      LOG.info("CommitLogServer selection strategy is SEQUENCE");
      listManager = new SequenceStrategy(this, numReplicas, pipePort);
    }
  }

  public void clear() {
    cacheLock.writeLock().lock();
    try {
      cache.clear();
    } finally {
      cacheLock.writeLock().unlock();
    }

    hasAlreadyBeenLoaded.set(false);

    listManagerLock.writeLock().lock();
    try {
      listManager.clear();
    } finally {
      listManagerLock.writeLock().unlock();
    }
  }

  public InetSocketAddress[] getAllAddresses() {
    listManagerLock.readLock().lock();
    try {
      return listManager.getAllLogServerAddressList();
    } finally {
      listManagerLock.readLock().unlock();
    }
  }

  public ServerSet getTabletServerSet(final String tabletName) throws IOException {
    cacheLock.readLock().lock();
    try {
      return cache.get(tabletName);
    } finally {
      cacheLock.readLock().unlock();
    }
  }
  
  public CloudataConf getConf() {
    return this.conf;
  }
  
  public ServerSet getReplicaAddresses(final String tabletName) throws IOException {
    ServerSet serverSet = null;
    boolean storeImage = false;

    cacheLock.readLock().lock();
    try {
      if ((serverSet = cache.get(tabletName)) != null) {
        return serverSet;
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    cacheLock.writeLock().lock();
    try {
      loadCacheFromImage(tabletName);

      if ((serverSet = cache.get(tabletName)) == null) {
        InetSocketAddress[] addrList = null;
        listManagerLock.readLock().lock();
        try {
          addrList = listManager.electCommitLogServers();
          printAddresses(tabletName, addrList);
        } finally {
          listManagerLock.readLock().unlock();
        }

        serverSet = new ServerSet(addrList);
        //LOG.info("map k[" + tabletName + "], v of hash[" + serverSet + "]");
        cache.put(tabletName, serverSet);
        storeImage = true;
      }
    } finally {
      cacheLock.writeLock().unlock();
    }

    if (storeImage) {
      final InetSocketAddress[] addresses = serverSet.getAddressList();
      
      new Thread() {
        public void run() {
          try {
            storeToImage(tabletName, addresses);
          } catch (IOException e) {
            LOG.warn("Fail to store Image", e);
          }
        }
      }.start();
    }

    return serverSet;
  }

  private void printAddresses(String tabletName, InetSocketAddress[] addrList) {
    if (LOG.isInfoEnabled()) {
      String addrMsg = "";
      for (int i = 0; i < addrList.length; i++) {
        addrMsg += addrList[i].getHostName() + ":" + addrList[i].getPort()
            + ", ";
      }
      LOG.debug("assign commitlog servers of tablet [" + tabletName + "] to "
          + addrMsg);
    }
  }

  public void removeReplica(String tabletName) throws IOException {
    LOG.info("remove commit log server info of tablet[" + tabletName + "]");

    removeFromImage(tabletName);

    cacheLock.writeLock().lock();
    try {
      cache.remove(tabletName);
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private void removeFromImage(String tabletName) throws IOException {
    GPath commitLogMetaPath = new GPath(logImagePath + "/" + tabletName);

    if (fs.exists(commitLogMetaPath)) {
      fs.delete(commitLogMetaPath);
    }
  }

  private void storeToImage(String tabletName, InetSocketAddress[] addrList)
      throws IOException {
    GPath commitLogMetaPath = new GPath(logImagePath + "/" + tabletName);

    if (fs.exists(commitLogMetaPath)) {
      if (isSameCommitLogInfo(commitLogMetaPath, addrList)) {
        LOG.info("Ignore deleting commit log meta path: "
            + (logImagePath + "/" + tabletName));
        return;
      } else {
        LOG.info("Deleting commit log meta path: "
            + (logImagePath + "/" + tabletName) + " and remake");
        fs.delete(commitLogMetaPath, true);
      }
    }

    // META 정보 생성
    OutputStream out = fs.create(commitLogMetaPath);
    for (InetSocketAddress eachServer : addrList) {
      out.write((eachServer.getHostName() + ":" + eachServer.getPort() + "\n")
          .getBytes());
    }
    out.close();
  }

  private boolean isSameCommitLogInfo(GPath commitLogMetaPath,
      InetSocketAddress[] addrList) throws IOException {
    BufferedReader br = null;

    try {
      br = new BufferedReader(new InputStreamReader(fs.open(commitLogMetaPath)));
      String line = null;

      while ((line = br.readLine()) != null) {
        int i = 0;
        for (; i < addrList.length; i++) {
          if (addrList.equals(NetworkUtil.getAddress(line))) {
            break;
          }
        }

        if (i == addrList.length) {
          return false;
        }
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }

    return true;
  }

  // should be called with write lock of cacheLock
  private void loadCacheFromImage(String tabletName) throws IOException {
    GPath tabletPath = new GPath(logImagePath, tabletName);
    if (fs.exists(tabletPath)) {
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(fs.open(tabletPath)));
        String line = null;
        List<InetSocketAddress> addrList = new ArrayList<InetSocketAddress>();
        while ((line = br.readLine()) != null) {
          addrList.add(NetworkUtil.getAddress(line));
        }

        if (!addrList.isEmpty()) {
          ServerSet serverSet = new ServerSet(addrList
              .toArray(new InetSocketAddress[0]));
          //LOG.info("map k[" + tabletName + "], v of hash[" + serverSet.hashCode());

          cache.put(tabletName, serverSet);
        }
      } finally {
        if (br != null) {
          br.close();
        }
      }
    }
  }

  private void initCloudataFileSystem(CloudataConf conf) throws IOException {
    fs = CloudataFileSystem.get(conf);
    GPath parentPath = new GPath(logImagePath);

    if (!fs.exists(parentPath)) {
      fs.mkdirs(parentPath);
    }
  }

  private void loadFromZooKeeper() throws IOException {
    if (hasAlreadyBeenLoaded.get() && !listManager.isEmpty()) {
      return;
    }

    listManagerLock.writeLock().lock();

    try {
      if (listManager.isEmpty() == false) {
        listManager.clear();
        runningServers.clear();
      }

      try {
        if(zk.exists(LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), false) == null) {
          LockUtil.createNodes(zk, LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), null, CreateMode.PERSISTENT);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      List<String> dirs = null;
      try {
        dirs = zk.getChildren(LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), new CommitLogServerWatcher());
      } catch (Exception e) {
        throw new IOException(e);
      }
      if (dirs == null || dirs.size() == 0) {
        throw new CommitLogException("No live commit log servers");
      } else if (dirs.size() < numReplicas) {
        throw new CommitLogException(
            "Insufficient number of commit log servers. "
                + "According to cloudata configuration, commitlog.num.replicas is "
                + numReplicas);
      }

      for (String eachServer: dirs) {
        listManager.addCommitLogServers(eachServer);
        runningServers.add(eachServer);
      }

      hasAlreadyBeenLoaded.set(true);
    } finally {
      listManagerLock.writeLock().unlock();
    }
  }

  protected boolean hasDiskCapacity(InetSocketAddress address) {
    DiskInfo diskInfo = diskInfos.get(address);
    if(diskInfo == null) {
      return true;
    }
    boolean result = diskInfo.getPercentUsed() < maxDiskUsage;
    if(!result) {
      LOG.debug(address + " no space");  
    }
    
    return result;
  }
  
  class CommitLogServerCapacityMonitor extends Thread {
    public void run() {
      while(true) {
        InetSocketAddress[]  addresses = listManager.getAllLogServerAddressList();
        
        for(InetSocketAddress eachAddress: addresses) {
          try {
            CommitLogServerIF server = (CommitLogServerIF)CRPC.getProxy(CommitLogServerIF.class,
                CommitLogServerIF.versionID, eachAddress, conf);
            DiskInfo diskInfo = server.getDiskInfo();
            
            diskInfos.put(eachAddress, diskInfo);
          } catch (Exception e) {
            LOG.warn("Can't get disk usage info:" + eachAddress);
          }
        }
        try {
          Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
          return;
        }
      } 
    }
  }
  
  class CommitLogServerWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if(event.getType() == Event.EventType.NodeChildrenChanged) {
        listManagerLock.writeLock().lock();
        try {
          List<String> serverLocks = null;
          try {
            serverLocks =  zk.getChildren(LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), this);
          } catch(NoNodeException e) {
          }
          
          if(serverLocks == null) {
            serverLocks = new ArrayList<String>();
          }
          
          Set<String> tmpServers = new HashSet<String>();
          tmpServers.addAll(runningServers);
          for(String eachServer: serverLocks) {
            if(!tmpServers.contains(eachServer)) {
              LOG.info("commit log server node [" + eachServer + "] is added");

              try {
                listManager.addCommitLogServers(eachServer);
                runningServers.add(eachServer);
              } catch (IOException e) {
                LOG.warn("adding commit log server [" + eachServer
                    + "] from list manager", e);
              }
            } else {
              tmpServers.remove(eachServer);
            }
          }
            
          
          for(String failedServer: tmpServers) {
            LOG.info("commit log server node [" + failedServer + "] is deleted");
            try {
              listManager.removeCommitLogServer(failedServer);
              runningServers.remove(failedServer);
            } catch (IOException e) {
              LOG.warn("removing commit log server [" + failedServer
                  + "] from list manager", e);
            }
          }
          
          notifyCLServerFailed(tmpServers);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        } finally {
          listManagerLock.writeLock().unlock();
        }
      }
    }

    private void notifyCLServerFailed(Set<String> failedServers) {
      if(tabletServer != null) {
        Map<String, ServerSet> tempCache = new HashMap<String, ServerSet>();
        cacheLock.readLock().lock();
        try {
          tempCache.putAll(cache);
        } finally {
          cacheLock.readLock().unlock();
        }
        
        List<String> errorTablets = new ArrayList<String>();
        for(Map.Entry<String, ServerSet> entry: tempCache.entrySet()) {
          for(InetSocketAddress address: entry.getValue().getAddressList()) {
            if(failedServers.contains(address.getHostName() + ":" + address.getPort())) {
              errorTablets.add(entry.getKey());
              break;
            }
          }
        }
        
        cacheLock.writeLock().lock();
        try {
          for(String eachTablet: errorTablets) {
            cache.remove(eachTablet);
          }
        } finally {
          cacheLock.writeLock().unlock();
        }

        //add stale set
        commitLogFs.processCommitLogServerFailed(failedServers, errorTablets);
        tabletServer.processCommitLogsServerFail(errorTablets);
      }
    }
  }
}

// // Server List Management Strategies ////

abstract class ServerListManagementStrategy {
  protected InetSocketAddress localCommitLogServerAddress;
  protected int numReplica;
  protected int pipePort;
  protected ServerLocationManager locationManager;
  protected boolean locality = true;
  
  protected ServerListManagementStrategy(ServerLocationManager locationManager,
      int numReplica, int pipePort) {
    this.numReplica = numReplica;
    this.pipePort = pipePort;
    this.locationManager = locationManager;
    this.locality = locationManager.getConf().getBoolean("commitLogServer.locality", true);
  }

  abstract InetSocketAddress[] electCommitLogServers() throws IOException;

  abstract boolean isEmpty();

  abstract InetSocketAddress[] getAllLogServerAddressList();

  abstract void clear();

  abstract void removeCommitLogServer(String hostAddr) throws IOException;

  abstract void addCommitLogServers(String hostAddr) throws IOException;

  static int getPort(String addr) {
    return Integer.valueOf(addr.substring(addr.indexOf(":") + 1));
  }

  static String getHost(String addr) {
    return addr.substring(0, addr.indexOf(":"));
  }
  
  static String getStringAddrFrom(InetSocketAddress addr) {
    return addr.getAddress().getHostAddress() + ":" + addr.getPort();
  }
}

class SequenceStrategy extends ServerListManagementStrategy {
  static final Log LOG = LogFactory.getLog(SequenceStrategy.class);

  SequenceStrategy(ServerLocationManager locationManager, int numReplica, int pipePort) {
    super(locationManager, numReplica, pipePort);
  }

  TreeSet<InetSocketAddress> addrSet = new TreeSet<InetSocketAddress>(
      new InetSocketAddressComparator());

  void addCommitLogServers(String hostAddrStr) throws IOException {
    InetSocketAddress hostAddr = new InetSocketAddress(InetAddress
        .getByName(getHost(hostAddrStr)), getPort(hostAddrStr));

    if (hostAddr.getAddress().equals(InetAddress.getLocalHost())) {
      localCommitLogServerAddress = hostAddr;
    }

    LOG.info("CommitLogServer [" + hostAddrStr + "] is ready");
    addrSet.add(hostAddr);
  }

  void clear() {
    addrSet.clear();
  }

  InetSocketAddress[] electCommitLogServers() throws IOException {
    if (numReplica > addrSet.size()) {
      throw new IOException(
          "Cannot elect commitlog server!! the num of available servers are "
              + addrSet.size());
    }

    InetSocketAddress target = new InetSocketAddress(InetAddress.getLocalHost(), pipePort);

    NavigableSet<InetSocketAddress> retSet = addrSet.tailSet(target, true);

    int index = 0;

    Iterator<InetSocketAddress> iter = retSet.iterator();
    InetSocketAddress[] ret = new InetSocketAddress[numReplica];

    while (iter.hasNext() && index < numReplica) {
      InetSocketAddress addr = iter.next();
      if(locationManager.hasDiskCapacity(addr)) {
        ret[index] = addr;
      }
    }

    if (index < numReplica) {
      Iterator<InetSocketAddress> headIter = addrSet.iterator();
      while (headIter.hasNext() && index < numReplica) {
        InetSocketAddress addr = headIter.next();
        if(locationManager.hasDiskCapacity(addr)) {
          ret[index++] = headIter.next();
        }
      }
    }

    if(ret.length < numReplica) {
      throw new IOException(
          "Cannot elect commitlog server!! the num of elected servers are "
              + ret.length + ", check commitlog server's disk usage");
    }
    return ret;
  }

  InetSocketAddress[] getAllLogServerAddressList() {
    return addrSet.toArray(new InetSocketAddress[0]);
  }

  boolean isEmpty() {
    return addrSet.isEmpty();
  }

  void removeCommitLogServer(String hostAddrStr) throws IOException {
    InetSocketAddress hostAddr = new InetSocketAddress(InetAddress
        .getByName(getHost(hostAddrStr)), getPort(hostAddrStr));

    addrSet.remove(hostAddr);
    
    if (hostAddr.getAddress().equals(InetAddress.getLocalHost())) {
      localCommitLogServerAddress = null;
    }
  }
}

class RandomStrategy extends ServerListManagementStrategy {
  static final Log LOG = LogFactory.getLog(RandomStrategy.class);
  
  Random rand  = new Random(System.currentTimeMillis());

  protected ArrayList<InetSocketAddress> logServerAddrList = new ArrayList<InetSocketAddress>();

  RandomStrategy(ServerLocationManager locationManager, int numReplica, int pipePort) {
    super(locationManager, numReplica, pipePort);
  }

  public InetSocketAddress[] getAllLogServerAddressList() {
    return logServerAddrList.toArray(new InetSocketAddress[logServerAddrList
        .size()]);
  }

  public boolean isEmpty() {
    return logServerAddrList.isEmpty();
  }

  public void clear() {
    logServerAddrList.clear();
  }

  public void addCommitLogServers(String hostAddrStr) throws IOException {
    InetSocketAddress hostAddr = new InetSocketAddress(InetAddress
        .getByName(getHost(hostAddrStr)), getPort(hostAddrStr));

    if (hostAddr.getAddress().equals(InetAddress.getLocalHost())) {
      if(locality) {
        localCommitLogServerAddress = hostAddr;
      }
    }

    LOG.info("CommitLogServer [" + hostAddrStr + "] is ready");
    logServerAddrList.add(hostAddr);
  }

  public InetSocketAddress[] electCommitLogServers() throws IOException {
    InetSocketAddress[] ret = new InetSocketAddress[numReplica];

    if (numReplica > logServerAddrList.size()) {
      throw new IOException(
          "Cannot elect commitlog server!! the num of available servers are "
              + logServerAddrList.size());
    }

    int i = 0;
    if (localCommitLogServerAddress != null && 
        locationManager.hasDiskCapacity(localCommitLogServerAddress)) {
      ret[0] = localCommitLogServerAddress;
      i = 1;
    }

    int prevIndex = -1;
    while (i < numReplica) {
      int index = Math.abs(rand.nextInt()) % logServerAddrList.size();

      if (prevIndex == index) {
        continue;
      }

      InetSocketAddress selected = logServerAddrList.get(index);
      prevIndex = index;
      if (isNew(ret, selected) && locationManager.hasDiskCapacity(selected)) {
        ret[i++] = selected;
      }
    }

    if(ret.length < numReplica) {
      throw new IOException(
          "Cannot elect commitlog server!! the num of elected servers are "
              + ret.length + ", check commitlog server's disk usage");
    }
    
    return ret;
  }

  private boolean isNew(InetSocketAddress[] addresses,
      InetSocketAddress selected) {
    for (int i = 0; i < addresses.length; i++) {
      if (addresses[i] == null) {
        return true;
      } else if (addresses[i].equals(selected)) {
        return false;
      }
    }
    return true;
  }

  public void removeCommitLogServer(String hostAddrStr) throws IOException {
    InetSocketAddress hostAddr = new InetSocketAddress(InetAddress
        .getByName(getHost(hostAddrStr)), getPort(hostAddrStr));

    if (hostAddr.getAddress().equals(InetAddress.getLocalHost())) {
      localCommitLogServerAddress = null;
    }
    logServerAddrList.remove(hostAddr);
    LOG.info("CommitLogServer [" + hostAddrStr + "] removed");
  }
  
  public static class InetSocketAddressComparator implements Comparator<InetSocketAddress> {
    public int compare(InetSocketAddress arg0, InetSocketAddress arg1) {
      byte[] b0 = arg0.getAddress().getAddress();
      byte[] b1 = arg1.getAddress().getAddress();

      for (int i = 0; i < b0.length; i++) {
        if (b0[i] != b1[i]) {
          return b0[i] < b1[i] ? -1 : 1;
        }
      }

      return arg0.getPort() - arg1.getPort();
    }
  }
}

