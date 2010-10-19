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
package org.cloudata.core.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.CTimeoutException;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.master.CloudataMaster;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.ColumnCollection;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletManagerProtocol;
import org.cloudata.core.tabletserver.TabletServer;

import junit.framework.TestCase;


public abstract class CloudataClusterTestCase extends TestCase {
  static {
    StaticTestEnvironment.initialize();
  }

  protected static int MAX_READY_TIME = 30; // 30 sec

  protected static MiniCloudataCluster cluster;

  protected static CloudataConf conf;

  protected static String errorMessage = "";

  static {
    conf = new CloudataConf();
    conf.set("testmode", "true");
    conf.set("maxTabletSize", 10);
    conf.set("memory.maxTabletSize", 20);
    conf.set("memory.maxTabletSizePerTablet", 5);

    try {
      cluster = new MiniCloudataCluster(conf, 1, 3, true);
      int count = 0;
      while (true) {
        if (CloudataMaster.isClusterReady()) {
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        count++;
        if (count > MAX_READY_TIME) {
          errorMessage += "exceeds MAX_READY_TIME\t";
        }
      }
    } catch (Exception e) {
      errorMessage += e.getMessage();
    }
  }

  protected ZooKeeper zk;

  protected CloudataClusterTestCase(String name) {
    super(name);
    try {
      conf.setInt("cloudata.cluster.zk.sessionTime", 180 * 1000);
      zk = LockUtil.getZooKeeper(conf, "TestCase", null);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (errorMessage.length() > 0) {
      fail(errorMessage);
    }
  }

  @Override
  public void tearDown() throws Exception {
  }

  protected Object[] findTabletInfo(String tableName) throws IOException {
    long startTime = System.currentTimeMillis();
    int timeout = 1 * 60 * 1000; // 1min
    Map<String, TabletServer> tabletServers = cluster.getTabletServers();
    TabletServer tabletServer = null;
    TabletInfo tablet = null;
    for (Map.Entry<String, TabletServer> entry : tabletServers.entrySet()) {
      TabletServer eachTabletServer = entry.getValue();
      TabletInfo[] tabletInfos = eachTabletServer.reportTablets();
      if (tabletInfos == null) {
        continue;
      }
      for (TabletInfo eachTabletInfo : tabletInfos) {
        if (tableName.equals(eachTabletInfo.getTableName())) {
          tabletServer = eachTabletServer;
          tablet = eachTabletInfo;
          break;
        }
      }
      if ((System.currentTimeMillis() - startTime) > timeout) {
        throw new CTimeoutException(
            "Timeout call CloudataClusterTestCase.findTabletInfo: " + tableName);
      }
    }
    return new Object[] { tabletServer, tablet };
  }

  /**
   * Tablet 정보가 META에 저장될때 까지 기다린다. MasterServer fail이 특정 table이 META에 저장된 다음에
   * 처리해야 할 경우 사용한다.
   * 
   * @param table
   * @throws IOException
   */
  protected void waitUntilSaveToMetaTablet(CTable ctable) throws IOException {
    long startTime = System.currentTimeMillis();
    int timeout = 1 * 60 * 1000; // 1min

    boolean end = false;
    while (end) {
      try {
        TabletInfo[] tabletInfos = ctable.listTabletInfos();
        if (tabletInfos == null || tabletInfos.length == 0) {
          continue;
        }

        Object[] metaInfo = findTabletInfo(Constants.TABLE_NAME_META);
        if (metaInfo == null || metaInfo.length == 0) {
          continue;
        }
        TabletServer metaTabletServer = (TabletServer) metaInfo[0];
        TabletInfo metaTablet = (TabletInfo) metaInfo[1];
        Tablet metaTable = metaTabletServer.getTablet(metaTablet
            .getTabletName());
        ColumnCollection columnCollection = metaTable.getMemorySSTable()
            .getColumnCollections().get(Constants.META_COLUMN_NAME_TABLETINFO);
        if (columnCollection == null) {
          continue;
        }

        end = false;
        for (TabletInfo eachTabletInfo : tabletInfos) {
          Row.Key rowKey = Tablet.generateMetaRowKey(eachTabletInfo
              .getTableName(), eachTabletInfo.getEndRowKey());
          ColumnValue[] columnValues = columnCollection.get(rowKey);
          if (columnValues == null || columnValues.length == 0) {
            continue;
          }
          if (!eachTabletInfo.getAssignedHostName().equals(
              columnValues[0].getValueAsString())) {
            end = false;
          } else {
            end = true;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      if ((System.currentTimeMillis() - startTime) > timeout) {
        throw new CTimeoutException(
            "Timeout call CloudataClusterTestCase.waitTabletAssign: "
                + ctable.descTable());
      }
    }
  }

  protected List<TabletInfo> listTabletInfos(String tableName) {
    List<TabletInfo> result = new ArrayList<TabletInfo>();
    Map<String, TabletServer> tabletServers = cluster.getTabletServers();
    TabletServer tabletServer = null;
    TabletInfo tablet = null;
    for (Map.Entry<String, TabletServer> entry : tabletServers.entrySet()) {
      TabletServer eachTabletServer = entry.getValue();
      TabletInfo[] tabletInfos = eachTabletServer.reportTablets();
      if (tabletInfos == null) {
        continue;
      }
      for (TabletInfo eachTabletInfo : tabletInfos) {
        if (tableName.equals(eachTabletInfo.getTableName())) {
          result.add(eachTabletInfo);
        }
      }
    }
    return result;
  }

  protected TabletManagerProtocol connectTabletManager(
      String tabletServerHostName, CloudataConf conf) throws IOException {
    return (TabletManagerProtocol) CRPC.getProxy(TabletManagerProtocol.class,
        TabletManagerProtocol.versionID, NetworkUtil
            .getAddress(tabletServerHostName), conf);
  }

  protected DataServiceProtocol connectTabletServer(TabletInfo tabletInfo,
      CloudataConf conf) throws IOException {
    if (tabletInfo == null) {
      return null;
    }
    return connectTabletServer(tabletInfo.getAssignedHostName(), conf);
  }

  protected static DataServiceProtocol connectTabletServer(String hostName,
      CloudataConf conf) throws IOException {
    if (hostName == null) {
      return null;
    }
    DataServiceProtocol tabletServer = (DataServiceProtocol) CRPC.getProxy(
        DataServiceProtocol.class, DataServiceProtocol.versionID, NetworkUtil
            .getAddress(hostName), conf);

    return tabletServer;
  }
}
