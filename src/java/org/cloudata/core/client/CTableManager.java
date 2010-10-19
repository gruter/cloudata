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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.common.CloudataLock;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.ipc.CRemoteException;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.master.CloudataMaster;
import org.cloudata.core.master.TableManagerProtocol;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.TabletManagerProtocol;


/**
 * 테이블을 생성하거나 스키마의 변경, drop 등과 같은 DDL(data definit ion language)과 관련된 작업을 수행한다.
 * @author babokim
 *
 */
public class CTableManager implements Constants {
  private static final Log LOG = LogFactory.getLog(CTableManager.class.getName());
  
  static final CloudataLock lock = new CloudataLock();

  protected static TabletManagerProtocol connectTabletManager(String tabletServerHostName, CloudataConf conf) throws IOException {
    try {
      return (TabletManagerProtocol) CRPC.getProxy(TabletManagerProtocol.class,
          TabletManagerProtocol.versionID, NetworkUtil.getAddress(tabletServerHostName), conf);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }  
  
  public static DataServiceProtocol connectTabletServer(TabletInfo tabletInfo, CloudataConf conf) throws IOException {
    if(tabletInfo == null) {
      LOG.warn("Cannot connect to tablet server since tabletInfo is null.");
      return null;
    }
    return connectTabletServer(tabletInfo.getAssignedHostName(), conf);
  }
  
  public static DataServiceProtocol connectTabletServer(String hostName, CloudataConf conf) throws IOException {
    if(hostName == null) {
      return null;
    }
    DataServiceProtocol tabletServer = (DataServiceProtocol) CRPC.getProxy(DataServiceProtocol.class,
        DataServiceProtocol.versionID, NetworkUtil.getAddress(hostName), conf);
    
    return tabletServer;
  }
  
  public static IOException makeIOException(Exception e) {
    if(e instanceof CRemoteException) {
      CRemoteException re = (CRemoteException)e;
      IOException resultException = null;
      try {
        if(re.getClassName() == null) {
          throw re;
        }
        Constructor cn = IOException.class.getConstructor(String.class);
        cn.setAccessible(true);
        IOException ex = (IOException)cn.newInstance(e.getMessage());
        ex.initCause(e);
        return ex;
        //resultException = (IOException)Class.forName(re.getClassName()).newInstance();
        //resultException.initCause(e);
        //return resultException;
      } catch (Exception e1) {
        e1.printStackTrace();
        LOG.error(e1.getMessage());
//        return null;
        return re;
      }
    } else if(e instanceof IOException) {
      return (IOException)e;
    } else {
      IOException err = new IOException(e.getMessage());
      err.initCause(e);
      return err;
    }
  }
  
  /**
   * CloudataMaster에 연결한다.
   * @param conf
   * @return
   * @throws IOException
   */
  public static TableManagerProtocol getMasterServer(CloudataConf conf) throws IOException {
    ZooKeeper zk = TabletLocationCache.getInstance(conf).getZooKeeper();
    TableManagerProtocol masterServer = (TableManagerProtocol) CRPC.getProxy(TableManagerProtocol.class,
        TableManagerProtocol.versionID, NetworkUtil.getAddress(CloudataMaster.getMasterServerHostName(conf, zk)), conf);
    return masterServer;
  }
  
  /**
   * 테이블을 생성한다
   * @param conf
   * @param table
   * @throws IOException
   */
  protected static void createTable(CloudataConf conf, TableSchema table) throws IOException {
    createTable(conf, table, null);
  }

  /**
   * 테이블을 생성한다. 테이블 생성 후 전달된 Row.Key 정보를 이용하여 tablet을 만든다.
   * @param conf
   * @param table
   * @param endRowKeys
   * @throws IOException
   */
  protected static void createTable(CloudataConf conf, TableSchema table, Row.Key[] endRowKeys) throws IOException {
    checkReservedTable(table);
    try {
      TableManagerProtocol masterServer = getMasterServer(conf);
      masterServer.createTable(table, endRowKeys);
    } catch (IOException e) {
//      IOException err = RemoteExceptionHandler.decodeRemoteException(e);
//      if(!(err instanceof SocketTimeoutException)) {
//        throw err;
//      }
      
      throw e;
    }
  }
  
  /**
   * 테이블이 존재하는지 확인한다.
   * @param conf
   * @param tableName
   * @return 존재하면 true, 존재하지 않으면 false
   * @throws IOException
   */
  protected static boolean existsTable(CloudataConf conf, String tableName) throws IOException {
    TableSchema tableSchema = TableSchema.loadTableSchema(conf, TabletLocationCache.getInstance(conf).getZooKeeper(), tableName);
    return tableSchema != null;
  }
  
  /**
   * ROOT, META 테이블 등과 같이 시스템에서 사용하는 테이블인지 확인한다.
   * @param table
   * @throws IllegalArgumentException
   */
  private static void checkReservedTable(TableSchema table) throws IllegalArgumentException {
    if(TABLE_NAME_ROOT.equals(table.getTableName()) ||
        TABLE_NAME_META.equals(table.getTableName())) {
      throw new IllegalArgumentException("Table [" + table.getTableName() + "] is reserved table");
    }
  }
    
  
  /**
   * 모든 테이블 목록을 조회한다.
   * @param conf
   * @return TableInfo[]
   * @throws IOException
   */
  protected static TableSchema[] listTables(CloudataConf conf) throws IOException {
    TableSchema[] tables = getMasterServer(conf).listTables();
    if(tables == null || tables.length == 0) {
      return tables;
    }
   
    List<TableSchema> result = new ArrayList<TableSchema>();
    for(TableSchema eachTable: tables) {
      result.add(eachTable);
    }
    Collections.sort(result);
    return result.toArray(new TableSchema[result.size()]); 
  }
  

  /**
   * 테이블을 drop한다.
   * @param conf
   * @param tableName
   * @throws IOException
   */
  public static void dropTable(CloudataConf conf, String tableName)throws IOException {
    getMasterServer(conf).dropTable(tableName);
  }
}
