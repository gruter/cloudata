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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.CloudataLock;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchemaMap;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.Tablet;


/**
 * @author jindolk
 *
 */
public class TabletLocationCache {
  private static final Log LOG = LogFactory.getLog(TabletLocationCache.class.getName());
  
  private static TabletLocationCache instance;
  
  private CloudataConf conf;
  
  private TabletInfo rootTablet;
  
  private DataServiceProtocol rootTabletServer;
  
  //TableName -> TableInfo
  private TableSchemaMap tableSchemas;

  protected TreeMap<Row.Key, TabletInfo> rootCache = new TreeMap<Row.Key, TabletInfo>();

  protected TreeMap<Row.Key, TabletInfo> metaCache = new TreeMap<Row.Key, TabletInfo>();

  private final CloudataLock cacheLock = new CloudataLock();

  private String hostName;

  private ZooKeeper zk;

  private String zkKey;
  
  private Object zkConnectMonitor = new Object();
  
  public synchronized static TabletLocationCache getInstance(CloudataConf conf) throws IOException {
    if(instance == null) {
      instance = new TabletLocationCache(conf);
      
      try {
        instance.initRootTablet();
      } catch(IOException e) {
        LOG.warn("cannot load ROOT tablet or META tablet info:" + e.getMessage(), e);
      }
    }
    return instance;  
  }
  
  private TabletLocationCache(CloudataConf conf) throws IOException {
    this.conf = conf;
    connectZK();
  }
  
  private void connectZK() throws IOException {
    if(hostName == null) {
      hostName = InetAddress.getLocalHost().getHostName();
    }
    
    if(zkKey != null) {
      zk = null;
      LockUtil.closeZooKeeper(zkKey);
    }
    
    zkKey = "CloudataClient_" + hostName + "_" + System.currentTimeMillis();
    
    synchronized(zkConnectMonitor) {
      ZooKeeper tempZK = LockUtil.getZooKeeper(conf, zkKey, new ZKWatcher());
      try {
        zkConnectMonitor.wait();
      } catch (InterruptedException e) {
      }
      clearAllCache();
      zk = tempZK;
      if(tableSchemas == null) {
        tableSchemas = new TableSchemaMap(conf, zk);
      } else {
        tableSchemas.clear();
        tableSchemas.setZooKeeper(zk);
      }
    }
  }
  
  public ZooKeeper getZooKeeper() throws IOException {
    synchronized(zkConnectMonitor) {
      while(zk == null) {
        try {
          zkConnectMonitor.wait();
        } catch (InterruptedException e) {
        }
      }
    } 
    
    return zk;
  }
  
  public TableSchemaMap getTableInfos() {
    return tableSchemas;
  }
  
  protected TableSchema getTableSchema(String tableName) throws IOException {
    TableSchema tableSchema = null;
    
    if ((tableSchema = tableSchemas.get(tableName)) != null) {
      return tableSchema;
    }
    
    tableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
    if (tableSchema == null) {
      return null;
    }
    
    if (!tableSchemas.putIfAbsent(tableName, tableSchema)) {
      return tableSchemas.get(tableName);
    }
    
    return tableSchema;
  }
  
  public void clearAllCache() {
    cacheLock.obtainWriteLock();
    try {
      rootTablet = null;
      rootCache.clear();
      metaCache.clear();
    } finally {
      cacheLock.releaseWriteLock();
    }
    
    if(tableSchemas != null) {
      tableSchemas.clear();
    }
  }

  public void clearTabletCache(String tableName, TabletInfo tabletInfo) {
    if(tabletInfo == null) {
      return;
    }
    cacheLock.obtainWriteLock();
    try {
      if(Constants.TABLE_NAME_ROOT.equals(tableName)) {
        rootTablet = null;
      } else if(Constants.TABLE_NAME_META.equals(tableName)) {
        rootCache.clear();
        metaCache.clear();
      } else {
        TreeMap<Row.Key, TabletInfo> tempMetaCache = new TreeMap<Row.Key, TabletInfo>();
        tempMetaCache.putAll(metaCache);
        for(Map.Entry<Row.Key, TabletInfo> entry: tempMetaCache.entrySet()) {
          Row.Key rowKey = entry.getKey();
          TabletInfo entryTabletInfo = entry.getValue();
          if(entryTabletInfo.equals(tabletInfo)) {
            metaCache.remove(rowKey);
          }
        }
      }
    } finally {
      cacheLock.releaseWriteLock();
    }
  }
  
  public void clearTabletCache(String tableName, Row.Key rowKey, TabletInfo tabletInfo) {
    cacheLock.obtainWriteLock();
    try {
      if(Constants.TABLE_NAME_ROOT.equals(tableName)) {
        rootTablet = null;
      } else if(Constants.TABLE_NAME_META.equals(tableName)) {
        if(tabletInfo == null) {
          //LOG.debug("clear all root cache");
          rootCache.clear();
        } else {
          removeFromCache(rootCache, Tablet.generateMetaRowKey(Constants.TABLE_NAME_META, rowKey), tabletInfo);
        }
      } else {
        if(tabletInfo == null) {
          //LOG.debug("clear all meta cache");
          metaCache.clear();
        } else {
          removeFromCache(metaCache, Tablet.generateMetaRowKey(tableName, rowKey), tabletInfo);
        }
      }
    } finally {
      cacheLock.releaseWriteLock();
    }
  }
  
  public void removeTableSchemaCache(String tableName) {
    tableSchemas.remove(tableName);
  }

  protected TableSchema reloadTableSchema(String tableName) throws IOException {
    TableSchema tableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
    tableSchemas.override(tableName, tableSchema);
    return tableSchema;
  }
  
  protected TabletInfo lookupRootTablet() throws IOException {
    initRootTablet();
    return rootTablet;
  }
  
  /**
   * ROOT Tablet에서 META Tablet 정보를 가져온다.
   * @param tableName
   * @param rowKey
   * @return
   * @throws IOException
   */
  protected TabletInfo lookupMetaTablet(String tableName, Row.Key rowKey) throws IOException {
    Row.Key rootTableRowKey = Tablet.generateMetaRowKey(Constants.TABLE_NAME_META, rowKey);
    //cache에서 찾는다.
    TabletInfo tabletInfo = findFromCache(tableName, rootCache, rootTableRowKey, rowKey);
    if(tabletInfo != null) {
      //Cache에 있는 경우
      return tabletInfo;
    }
    
    //없으면 직접 TabletServer로 요청
    if(tabletInfo == null) {
      //LOG.debug("Root Cache miss:" + tableName + "," + rowKey);
      initRootTablet();
      if(rootTabletServer == null) {
        clearAllCache();
        return null;
      }
      try {
        //ROOT Tablet으로부터 META Tablet 정보를 가져온다.
        ColumnValue columnValue = rootTabletServer.getCloestMetaData(rootTablet.getTabletName(), rootTableRowKey);
        if(columnValue == null) {
          clearTabletCache(rootTablet.getTableName(), rootTableRowKey, rootTablet);
          return null;
        }
        tabletInfo = new TabletInfo();
        tabletInfo.readFields(columnValue.getValue());
        
        addCache(rootCache, 
            Tablet.generateMetaRowKey(Constants.TABLE_NAME_META, tabletInfo.getEndRowKey()),
            tabletInfo);
      } catch (Exception e) {
        //ROOT를 관리하는 TabletServer에 연결 중 장애가 발생하면 NULL을 반환
        //null을 받은 쪽에서 다시 retry할 때 다시 정보를 가져온다.
        //clearTabletCache(rootTablet.getTableName(), rootTableRowKey, rootTablet);
        clearAllCache();
        return null;
      }
    }
    return tabletInfo;
  }

  protected TabletInfo lookupUserTablet(String tableName, Row.Key rowKey) throws IOException {
    Row.Key metaTableRowKey = Tablet.generateMetaRowKey(tableName, rowKey);
    TabletInfo tabletInfo = findFromCache(tableName, metaCache, metaTableRowKey, rowKey);
    
    if(tabletInfo == null) {
      //LOG.debug("Cache missed:" + tableName + "," + rowKey + "," + metaTableRowKey);
      //META Tablet을 먼저 찾는다.
      TabletInfo metaTablet = lookupMetaTablet(Constants.TABLE_NAME_META, metaTableRowKey);

      if(metaTablet == null) {
        //root cache clear
        //clearTabletCache(Constants.TABLE_NAME_META, metaTableRowKey, null);
        clearAllCache();
        LOG.warn("Can't find meta tablet in looking up user tablet");
        return null;
      }
      
      //UserTablet을 찾는다.
      try {
        DataServiceProtocol metaTabletServer = CTableManager.connectTabletServer(metaTablet, conf);
        ColumnValue columnValue = metaTabletServer.getCloestMetaData(metaTablet.getTabletName(), metaTableRowKey);
        
        if(columnValue == null) {
          clearTabletCache(Constants.TABLE_NAME_META, metaTableRowKey, metaTablet);
          return null;
        }
        tabletInfo = new TabletInfo();
        tabletInfo.readFields(columnValue.getValue());

        if(tableName.equals(tabletInfo.getTableName())) {
          //System.out.println("Add Cache:" + rowKey + "," + tabletInfo);
          addCache(metaCache, Tablet.generateMetaRowKey(tableName, tabletInfo.getEndRowKey()), tabletInfo);
          return tabletInfo;
        } else {
          clearTabletCache(Constants.TABLE_NAME_META, metaTableRowKey, metaTablet);
          return null;
        }
      } catch (Exception e) {
        LOG.warn("Exception in reading meta:" + e.getMessage());
        clearAllCache();
        return null;
      }
    } else {
      //LOG.debug("Cache hits:" + tableName + "," + rowKey + "," + tabletInfo);
      return tabletInfo;
    }
  }
  
  private void addCache(TreeMap<Row.Key, TabletInfo> cache, Row.Key cacheRowKey, TabletInfo tabletInfo) {
    cacheLock.obtainWriteLock();
    try {
      cache.put(cacheRowKey, tabletInfo);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }
  
  private void initRootTablet() throws IOException {
    cacheLock.obtainWriteLock();
    try {
      if(rootTablet == null) {
        rootTablet = Tablet.getRootTabletInfo(conf, zk);
        if(rootTablet == null || rootTablet.getAssignedHostName() == null) {
          throw new IOException("Root Tablet is : " + rootTablet);
        }
        rootTabletServer = CTableManager.connectTabletServer(rootTablet, conf);
        if(!rootTabletServer.isServicedTablet(rootTablet.getTabletName())) {
          LOG.info("Invalid ROOT tablet server:" + rootTablet);
          rootTablet = null;
          rootTabletServer = null;
          return;
        }
      } 
    } catch (Exception e) {
      rootTablet = null;
      rootTabletServer = null;
      LOG.debug("Error initRootTablet:" + e.getMessage(), e);
      throw new IOException("Error initRootTablet:" + e.getMessage(), e);
    } finally {
      cacheLock.releaseWriteLock();
    }    
    preLoadFromRootTablet();
  }
  
  /**
   * ROOT Tablet에 있는 META Tablet 정보를 미리 로딩 한다.
   * @throws IOException
   */
  private void preLoadFromRootTablet() throws IOException {
    TableScanner scanner = null;
    
    try {
      scanner = ScannerFactory.openScanner(conf, rootTablet, Constants.META_COLUMN_NAME_TABLETINFO);
      ScanCell scanCell = null;
      
      int count = 0;
      
      while( (scanCell = scanner.next()) != null ) {
        TabletInfo tabletInfo = new TabletInfo();
        tabletInfo.readFields(scanCell.getBytes());
        
        Row.Key metaFormatRowKey = Tablet.generateMetaRowKey(Constants.TABLE_NAME_META, tabletInfo.getEndRowKey());
        addCache(rootCache, metaFormatRowKey, tabletInfo);
        
        count++;
        if(count > 1000) {
          break;
        }
      }
    } finally {
      if(scanner != null) {
        scanner.close();
      }
    }
  }
  
  /**
   * 클라이언트가 처음 로딩될때 META에 있는 일부 정보(약 10000건 정도)를 로딩 시킨다.
   * @throws IOException
   */
  private void preLoadFromMetaTablet() throws IOException {
    List<TabletInfo> rootTablets = new ArrayList<TabletInfo>();
    cacheLock.obtainReadLock();
    try {
      rootTablets.addAll(rootCache.values());
    } finally {
      cacheLock.releaseReadLock();
    }
    
    int count = 0;
    for(TabletInfo eachRootTablet: rootTablets) {
      TableScanner scanner = null;
      
      try {
        scanner = ScannerFactory.openScanner(conf, 
            eachRootTablet, Constants.META_COLUMN_NAME_TABLETINFO);
        ScanCell scanCell = null;
        
        while( (scanCell = scanner.next()) != null ) {
          TabletInfo tabletInfo = new TabletInfo();
          tabletInfo.readFields(scanCell.getBytes());
          
          Row.Key metaFormatRowKey = Tablet.generateMetaRowKey(tabletInfo.getTableName(), tabletInfo.getEndRowKey());
          addCache(metaCache, metaFormatRowKey, tabletInfo);
          
          count++;
          if(count > 10000) {
            break;
          }
        }
      } finally {
        if(scanner != null) {
          scanner.close();
        }
      }
    }
  }
  
  private void removeFromCache(TreeMap<Row.Key, TabletInfo> cache, Row.Key cacheRowKey, TabletInfo removeTablet) {
    if(cache.containsKey(cacheRowKey)) {
      cache.remove(cacheRowKey);
    }
    
    SortedMap<Row.Key, TabletInfo> tailMap = cache.tailMap(cacheRowKey);
    if(tailMap.isEmpty()) {
      return;
    }
    Row.Key tailFirst = tailMap.firstKey();
    
    TabletInfo tabletInfo = tailMap.get(tailFirst);
    
    if(tabletInfo.equals(removeTablet)) {
      cache.remove(tailFirst);
    } 
  }
  
  protected TabletInfo findFromCache(String tableName, TreeMap<Row.Key, TabletInfo> cache, Row.Key cacheRowKey, Row.Key dataRowKey) {
    cacheLock.obtainReadLock();
    try {
      if(cache.containsKey(cacheRowKey)) {
        TabletInfo tabletInfo = cache.get(cacheRowKey);
        if(tabletInfo.belongRowRange(cacheRowKey)) {
          return tabletInfo;
        } else {
          return null;
        }
      }

      SortedMap<Row.Key, TabletInfo> tailMap = cache.tailMap(cacheRowKey);
      if(tailMap.isEmpty()) {
        return null;
      }
      Row.Key tailFirst = tailMap.firstKey();
      
      TabletInfo tabletInfo = tailMap.get(tailFirst);
      if(tableName.equals(tabletInfo.getTableName()) && tabletInfo.belongRowRange(dataRowKey)) {
        return tabletInfo;
      } else {
        return null;
      }      
    } finally {
      cacheLock.releaseReadLock();
    }
  }
  
  class ZKWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if(event.getType() == Event.EventType.None) {
        switch (event.getState()) {
        case SyncConnected:
          LOG.info("ZK Connected");
          synchronized(zkConnectMonitor) {
            zkConnectMonitor.notifyAll();
          }
          break;
        case Disconnected:
          LOG.warn("ZK Disconnected");
          break;
        case Expired: 
          try {
            LOG.warn("ZK Expired. Try ZK Connecting");
            connectZK();
          } catch (Exception e) {
            LOG.fatal(e.getMessage(), e);
            //System.exit(0);
          }
          break;
        }
      }       
    }
  }
}
