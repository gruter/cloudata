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
package org.cloudata.core.common.lock;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritable;


/**
 * @author jindolk
 *
 */
public class LockUtil {
  static final Log LOG = LogFactory.getLog(LockUtil.class);

  static Map<String, ZooKeeper> zooKeepers = new HashMap<String, ZooKeeper>();
  
  public static String getZKPath(CloudataConf conf, String path) {
    if(path != null && path.length() == 0) {
      return "/" + conf.get("cloudata.cluster.name", "cloudata");
    }
    if(path.startsWith("/")) {
      return "/" + conf.get("cloudata.cluster.name", "cloudata") + path;
    } else {
      return "/" + conf.get("cloudata.cluster.name", "cloudata") + "/" + path;
    }
  }

  public static void delete(ZooKeeper zk, String path) throws IOException {
    try {
      zk.delete(path, -1);
    } catch(NoNodeException e) {
      return;
    } catch (Exception e) {
      LOG.error("Delete error:" + path + "," + e.getMessage());
      throw new IOException(e);
    }
  }
  
  public static synchronized ZooKeeper getZooKeeper(CloudataConf conf, String key, Watcher watcher) throws IOException {
    if(zooKeepers.containsKey(key)) {
      return zooKeepers.get(key);
    } else {
      String zkServer = conf.get("cloudata.cluster.zkservers");
      if(zkServer == null) {
        throw new IOException("No cloudata.cluster.zkservers property.");
      }
      ZooKeeper zk = new ZooKeeper(zkServer, 
            conf.getInt("cloudata.cluster.zk.sessionTime", 15 * 1000), watcher == null ? new DefaultWatcher() : watcher);
      zooKeepers.put(key, zk);
      return zk;
    }
  }

  public static synchronized void closeZooKeeper(String key) throws IOException {
    ZooKeeper zk = zooKeepers.remove(key);
    if(zk == null) {
      return;
    }
    
    try {
      zk.close();
    } catch (InterruptedException e) {
    }
  }
  
  public static void delete(ZooKeeper zk, String path, boolean recursive) throws IOException {
    try {
      if (zk.exists(path, false) == null) {
        return;
      }
      
      List<String> children = null;
      try {
        children = zk.getChildren(path, false);
      } catch(NoNodeException e) {
      }
      
      if (recursive && children != null && children.size() > 0) {
        for (String child: children) {
          delete(zk, path + "/" + child, recursive);
        }
      }
      delete(zk, path);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  public static String createNodes(ZooKeeper zk, String path, byte[] data, 
      CreateMode createMode) throws IOException{
    return createNodes(zk, path, data, createMode, false);
  }
  
  public static String createNodes(ZooKeeper zk, String path, byte[] data, 
      CreateMode createMode, boolean ignoreExists) throws IOException {
    
    try {
      String[] tokens = path.split("/");
      
      String currentPath = "/";
      for(int i = 0; i < tokens.length - 1; i++) {
        currentPath += tokens[i];
        try {
          zk.create(currentPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
        }
        if(tokens[i].length() > 0) {
          currentPath += "/";
        }
      }
      
      LOG.debug("Create node:" + path);
      return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode);
    } catch (Exception e) {
      if(ignoreExists && e instanceof NodeExistsException) {
        return "";
      }
      throw new IOException(e);
    }
  }
  
  public static byte[] getBytes(CWritable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    writable.write(new DataOutputStream(out));
    
    return out.toByteArray();
  }
  
  public static String getPathName(String path) {
    if(path == null) {
      return null;
    }
    
    int index = path.lastIndexOf("/");
    if(index < 0) {
      return null;
    }
    
    return path.substring(index + 1);
  }

  public static String acquireLock(ZooKeeper zk, String path) throws IOException {
    try {
      String createdPath = zk.create(path + "/Lock-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      String[] tokens = createdPath.split("/");
      
      String createdPathName = tokens[tokens.length -1];
  
      List<String> nodes = zk.getChildren(path, false);
      if(nodes == null || nodes.size() == 0) {
        throw new IOException("No child node:" + path);
      }
      
      Collections.sort(nodes);
      
      if(nodes.get(0).equals(createdPathName)) {
        LOG.debug("Get lock:" + path + "," + createdPathName);
        return createdPathName;
      } else {
        LOG.debug("Can't get lock:" + path + "," + createdPathName);
        delete(zk, createdPath);
        return null;
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  public static void releaseLock(ZooKeeper zk, String path, String lockKey) throws IOException {
    if(lockKey == null) {
      return;
    }
    try {
      LOG.debug("Release lock:" + path + "," + lockKey);

      delete(zk, path + "/" + lockKey);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  public static class DefaultWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      
    }
  }
}
