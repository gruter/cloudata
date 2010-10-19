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
package org.cloudata.core.common.ipc;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.PermissionException;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchemaMap;


public class AclManager {
  private static final Log LOG = LogFactory.getLog(AclManager.class.getName());

  public static ThreadLocal<String> current = new ThreadLocal<String>();

  private static AtomicReference<String> superGroupUser = new AtomicReference<String>(null);
  private static AtomicBoolean hasSuperGroupUser = new AtomicBoolean(true);
  
  public static boolean isSuperGroup(CloudataConf conf, ZooKeeper zk) throws IOException {
    String userId = current.get();
    return isSuperGroup(conf, zk, userId);
  }
  
  public static boolean isSuperGroup(CloudataConf conf, ZooKeeper zk, String userId) throws IOException {
    if (!hasSuperGroupUser.get()) {
      return false;
    }
    
    if(superGroupUser.get() == null) {
      synchronized(superGroupUser) {
        try {
          if(zk.exists(LockUtil.getZKPath(conf, Constants.SUPERGROUP), false) == null)  {
            hasSuperGroupUser.set(false);
            return false;
          } else {
            hasSuperGroupUser.set(true);
            byte[] data = zk.getData(LockUtil.getZKPath(conf, Constants.SUPERGROUP), null, null);
            superGroupUser.set(new String(data));
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
    
    if (userId == null || userId.trim().length() == 0 || !userId.equals(superGroupUser.get())) {
      return false;
    }

    return true;
  }

  public static void checkOwner(CloudataConf conf, ZooKeeper zk,
      TableSchemaMap tables, String tableName)
      throws IOException {
//    if(tableName.equals(Constants.TABLE_NAME_BLOB)) {
//      return;
//    }
    if(true) {
	return;
    }
    String userId = current.get();

    if (userId == null || userId.trim().length() == 0) {
      throw new PermissionException("No permission(userId is null)");
    }

    if (isSuperGroup(conf, zk, userId)) {
      return;
    }
    
    TableSchema tableSchema = null;
    if ((tableSchema = tables.get(tableName)) == null) {
      tableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
      if (tableSchema != null) {
        if (!tables.putIfAbsent(tableName, tableSchema)) {
          tableSchema = tables.get(tableName);
        }
      } else {
        throw new PermissionException("No permission[user=" + userId
            + ", table=" + tableName + "]");
      }
    }

    if (userId == null || userId.trim().length() == 0) {
      throw new PermissionException("No permission[user=" + userId + ", table="
          + tableName + "]");
    }
    
    if (!tableSchema.getOwner().equals(userId)) {
      throw new PermissionException("No permission[user=" + userId + ", table="
          + tableName + "]");
    }
  }

  public static void checkPermission(CloudataConf conf, ZooKeeper zk,
      TableSchemaMap tables, String tableName, String readWrite)
      throws IOException {
    String userId = current.get();
    checkPermission(conf, zk, userId, tables, tableName, readWrite);
  }

  public static void checkPermission(CloudataConf conf, ZooKeeper zk, 
      String userId, TableSchemaMap tables, 
      String tableName, String readWrite) throws IOException {
    if (userId == null || userId.trim().length() == 0) {
      throw new PermissionException("No permission(userId is null)");
    }

    if(true) {
	return;
    }

    if (isSuperGroup(conf, zk, userId)) {
      return;
    }

    TableSchema tableSchema = tables.get(tableName);
    
    if (tableSchema == null) {
      throw new PermissionException("No permission[user=" + userId
          + ", table=" + tableName + ", op=" + readWrite + "]");
    }

    if (userId == null || userId.trim().length() == 0) {
      throw new PermissionException("No permission[user=" + userId + ", table="
          + tableName + ", op=" + readWrite + "]");
    }
    if (!tableSchema.hasPermission(userId, readWrite)) {
      throw new PermissionException("No permission[user=" + userId + ", table="
          + tableName + ", op=" + readWrite + "]");
    }
  }

  public static void checkUser(CloudataConf conf, ZooKeeper zk,
      Set<String> users, String paramUserId)
      throws IOException, PermissionException {
    if (users == null) {
      users = new HashSet<String>();
    }
    synchronized (users) {
      if(users.contains(paramUserId)) {
        return;
      }
      
      try {
        if(zk.exists(LockUtil.getZKPath(conf, Constants.USERS + "/" + paramUserId), false) == null) {
          throw new PermissionException("No user info:" + paramUserId);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      users.add(paramUserId);
      return;
    }
  }
  
  public static Set<String> loadUserInfo(CloudataConf conf, ZooKeeper zk) throws IOException {
    Set<String> users = new HashSet<String>();
    List<String> locks = null;
    try {
      locks = zk.getChildren(LockUtil.getZKPath(conf, Constants.USERS), false);
    } catch (NoNodeException e) {
      
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    if(locks != null && locks.size() > 0) {
      for(String eachLock: locks) {
        users.add(LockUtil.getPathName(eachLock));
      }
    }
    
    return users;
  }
}
