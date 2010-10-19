package org.cloudata.core.tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.TabletLocationCache;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.lock.LockUtil;


public class TableSchemaMap {
  static final Log LOG = LogFactory.getLog(TableSchemaMap.class);
  
  Map<String, TableSchema> map = new HashMap<String, TableSchema>();
  ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  CloudataConf conf;
  ZooKeeper zk;
  
  public TableSchemaMap(CloudataConf conf, ZooKeeper zk) {
    this.conf = conf;
    this.zk = zk;
  }
  
  public boolean putIfAbsent(String tableName, TableSchema table) {
    rwLock.writeLock().lock();
    try {
      if (map.containsKey(tableName)) {
        return false;
      }
      
      map.put(tableName, table);
    } finally {
      rwLock.writeLock().unlock();
    }

    return true;
  }

  public boolean contains(String tableName) {
    rwLock.readLock().lock();
    try {
      return map.containsKey(tableName);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public void setZooKeeper(ZooKeeper zk) {
    this.zk = zk;
  }
  
  public TableSchema get(String tableName) throws IOException {
    TableSchema ret = null;
    rwLock.readLock().lock();
    try {
      if ((ret = map.get(tableName)) != null) {
        return ret;
      }
    } finally {
      rwLock.readLock().unlock();
    }
    
    rwLock.writeLock().lock();
    try {
      if ((ret = map.get(tableName)) != null) {
        return ret;
      }
      
      if ((ret = TableSchema.loadTableSchema(conf, zk, tableName)) != null) {
        String tableLockPath = LockUtil.getZKPath(conf, Constants.PATH_SCHEMA + "/" + tableName);
        try {
          zk.exists(tableLockPath, new TableSchemaWatcher());
        } catch (KeeperException e) {
          LOG.error("Error registering schema watcher," + ret, e);
        } catch (InterruptedException e) {
          LOG.error("Error registering schema watcher," + ret, e);
        }
        
        map.put(tableName, ret); 
      }
      
      return ret;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  class TableSchemaWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      String path = event.getPath();
      if(path == null) {
        return;
      }
      int index = path.lastIndexOf("/");
      String tableName = path;
      if(index > 0) {
        tableName = path.substring(index + 1);
      }
      remove(tableName);
    }
  }
  
  public void remove(String tableName) {
    rwLock.writeLock().lock();
    try {
      map.remove(tableName);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public void load(List<TableSchema> schemaList) throws IOException {
    rwLock.writeLock().lock();
    try {
      for(TableSchema schema : schemaList) {
        map.put(schema.getTableName(), schema);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public Collection<TableSchema> values() {
    ArrayList<TableSchema> ret = null;
    rwLock.readLock().lock();
    try {
      Collection<TableSchema> values = map.values();
      ret = new ArrayList<TableSchema>(values.size());
      ret.addAll(values);
    } finally {
      rwLock.readLock().unlock();
    }
    
    return ret;
  }

  public void clear() {
    rwLock.writeLock().lock();
    try {
      map.clear();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public void override(String tableName, TableSchema tableSchema) {
    rwLock.writeLock().lock();
    try {
      map.put(tableName, tableSchema);
    } finally {
      rwLock.writeLock().unlock();
    }
  }
}
