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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.cloudata.core.commitlog.CommitLogServer;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.master.CloudataMaster;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.TabletServer;


public class MiniCloudataCluster implements Constants {
  private static final Logger LOG = Logger.getLogger(MiniCloudataCluster.class.getName());
  
  protected CloudataConf conf;
  protected CloudataFileSystem fs;

  protected MiniPipeBasedCommitLogCluster commitLogCluster;
  
  protected Map<String, TabletServer> tabletServers;
  protected Map<String, Thread> tabletServerThreads;
  protected Map<String, CloudataMaster> masterServers;
  protected Map<String, Thread> masterServerThreads;
  private GPath parentDir;
  private boolean format;
  
  public MiniCloudataCluster(CloudataConf conf,
                            int nMasterServer, 
                            int nTabletServer, 
                            boolean format) throws IOException {
    this.conf = conf;
    this.tabletServers = new HashMap<String, TabletServer>();
    this.tabletServerThreads = new HashMap<String, Thread>();
    this.masterServers = new HashMap<String, CloudataMaster>();
    this.masterServerThreads = new HashMap<String, Thread>();
    this.format = format;
    
    this.conf.set("cloudata.filesystem", "local");
    init(nMasterServer, nTabletServer);
  }
  
  private void init(int nMasterServer, 
                      int nTabletServer) throws IOException {
    if(this.format) {
      CloudataMaster.format(conf, true, "lock");
    }
    
    this.fs = CloudataFileSystem.get(conf);
    this.parentDir = new GPath(
        conf.get("cloudata.root", 
            System.getProperty("user.home") + File.separator + ".cloudata_root"));
    fs.mkdirs(parentDir);

    this.commitLogCluster = new MiniPipeBasedCommitLogCluster(conf);

    try {
      Thread.sleep(2 * 1000);
    } catch (InterruptedException e) {
    }

    if(this.format) {
      CloudataMaster.format(conf, true, "file");
    }

    // Create the master
    startMasterServers(nMasterServer);
   
    try {
      Thread.sleep(2 * 1000);
    } catch (InterruptedException e) {
    }

    startTabletServers(nTabletServer);
  }  
  
  private void startMasterServers(final int nMasterServer) throws IOException {
    int masterServerPort = conf.getInt("masterServer.port", 7000);
    for(int i = 0; i < nMasterServer; i++) {
      CloudataConf masterServerConf = new CloudataConf(conf);
      masterServerConf.set("cloudata.filesystem", "local");
      masterServerConf.set("masterServer.port", masterServerPort + i);
      masterServerConf.set("masterServer.handler.count", 4);
      CloudataMaster masterServer = new CloudataMaster();
      masterServer.init(masterServerConf);
      this.masterServers.put(masterServer.getHostName(), masterServer);
      Thread t = new Thread(masterServer, "MasterServer-" + this.masterServers.size());
      t.start();
      this.masterServerThreads.put(masterServer.getHostName(), t);
    }
  }
  
  private void startTabletServers(final int nTabletServer)
  throws IOException {
    int tabletServerPort = conf.getInt("tabletServer.port", 7001);
    int scannerPort = conf.getInt("tabletServer.scanner.port", 50101);
    
    for(int i = 0; i < nTabletServer; i++) {
      CloudataConf tabletServerConf = new CloudataConf(conf);
      tabletServerConf.set("cloudata.filesystem", "local");
      tabletServerConf.set("tabletServer.port", tabletServerPort + i);
      tabletServerConf.set("tabletServer.scanner.port", scannerPort + i);
      tabletServerConf.set("tabletServer.handler.count", 4);
      TabletServer tabletServer = new TabletServer();
      tabletServer.init(tabletServerConf);
      this.tabletServers.put(tabletServer.getHostName(), tabletServer);
      Thread t = new Thread(tabletServer, "TabletServer-" + this.tabletServers.size());
      t.start();
      this.tabletServerThreads.put(tabletServer.getHostName(), t);
    }
  }

  public CloudataMaster getActiveMaster() throws IOException {
    for(Map.Entry<String, CloudataMaster> entry: masterServers.entrySet()) {
      if(entry.getValue().isMasterElected()) {
        return entry.getValue();
      }
    }
    return null;
  }
  
  public void shutdown() throws IOException {

    for(Map.Entry<String, CloudataMaster> entry: masterServers.entrySet()) {
      entry.getValue().shutdown();
    }

    for(Thread masterServerThread: masterServerThreads.values()) {
      masterServerThread.interrupt();
    }
    
    for(Map.Entry<String, TabletServer> entry: tabletServers.entrySet()) {
      entry.getValue().shutdown();
    }


    for(Thread tabletServerThread: tabletServerThreads.values()) {
      tabletServerThread.interrupt();
    }
    commitLogCluster.shutdown();
    
    CRPC.stopClient();
    try {
      Thread.sleep(5 * 1000);
    } catch (InterruptedException e) {
    }  
  }

  public void stopTabletServer(String hostName) throws IOException {
    System.out.println("Stop tablet server:" + hostName);
    TabletServer tabletServer = tabletServers.get(hostName);
    if(tabletServer == null) {
      throw new IOException("No live tablet");
    }
    TabletInfo[] tabletInfos = tabletServer.reportTablets();
    for(TabletInfo tabletInfo: tabletInfos) {
      System.out.println("\t stopping service: " + tabletInfo);
    }

    tabletServer.shutdown();
    tabletServers.remove(hostName);
    
    tabletServerThreads.get(hostName).interrupt();
  }  
  
  /**
   * 현재 active한 CloudataMaster를 중지시킨다.
   * @throws IOException
   */
  public void stopMasterServer() throws IOException {
    LOG.debug("Active Master server shutdown");
    CloudataMaster master = getActiveMaster();
    if(master != null) {
      master.shutdown();
      masterServerThreads.get(master.getHostName()).interrupt();
    }

    try {
      Thread.sleep(5 * 100);
    } catch (InterruptedException e) {
    }
  }
  
  public Map<String, TabletServer> getTabletServers() {
    return tabletServers;
  }

  
  public CommitLogServer[] getCommitLogServers() {
    return commitLogCluster.serverList;
  }
}
