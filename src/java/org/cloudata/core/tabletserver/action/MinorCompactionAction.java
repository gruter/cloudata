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
package org.cloudata.core.tabletserver.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.IdGenerator;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.DiskSSTable;
import org.cloudata.core.tabletserver.MemorySSTable;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.TabletServer;
import org.cloudata.core.tabletserver.action.TabletSplitAction.TabletSplitFinishAction;


public class MinorCompactionAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(MinorCompactionAction.class.getName());
  
  public static final String ACTION_TYPE = "MinorCompactionAction";

  static List<String> checkActions = new ArrayList<String>();
  static {  
    checkActions.add(MinorCompactionAction.ACTION_TYPE);
    checkActions.add(TabletSplitAction.ACTION_TYPE);
    checkActions.add(TabletSplitFinishAction.ACTION_TYPE);    
    checkActions.add(ScannerAction.ACTION_TYPE);
    checkActions.add(TabletLockAction.ACTION_TYPE);
    checkActions.add(TabletDropAction.ACTION_TYPE);
  }
  
  protected Tablet tablet;
  protected TabletInfo tabletInfo;
  protected boolean forced;
  
  public MinorCompactionAction() {
    
  }
  
  public void init(CloudataConf conf, Tablet tablet) throws IOException {
    this.tablet = tablet;
    this.tabletInfo = tablet.getTabletInfo();
  }

  public boolean isWaitingMode(ActionChecker actionChecker) {
    return !(actionChecker.runningActions.contains(TabletSplitAction.ACTION_TYPE) ||
            actionChecker.runningActions.contains(TabletSplitFinishAction.ACTION_TYPE) ||
            actionChecker.waitingActions.contains(TabletSplitAction.ACTION_TYPE) ||
            actionChecker.waitingActions.contains(TabletSplitFinishAction.ACTION_TYPE));
  }

  public boolean isThreadMode() {
    return true;
  }

  public void setForced(boolean flag) {
    this.forced = flag;
  }
  
  public void run() {
    try {
      //tablet.setMinorCpmpactionLock(true);
      LOG.info("TabletMinorCompaction Start: " + tabletInfo.getTabletName() + "," + tablet.getTabletServer().getHostName());
      compact();
    } catch (Exception e) {
      LOG.error("TabletMinorCompaction error: " + tabletInfo.getTabletName() + ": " + e.getMessage(), e);
    } finally {
      if(forced) {
        tablet.getReady().compareAndSet(false, true);
      }
      forced = false;
      //tablet.setMinorCpmpactionLock(false);
    }
  }
  
  public void compact() throws IOException {
    if(!forced && !tablet.isReady()) {
      tablet.getActionChecker().endAction(this);
      LOG.info("TabletMinorCompaction End: " + tablet.getTabletInfo().getTabletName() + " cause by tablet is not ready");
      tablet.clearCommitLogErrorState();
      return;
    }
    
    MemorySSTable memorySSTable = tablet.getMemorySSTable();
    
    long startTime = System.currentTimeMillis();
    //minor compaction 처리 중에 memory tablet freeze 시키고 빈 memory tablet 할당할 동안은 commit 처리되면 안됨
    DiskSSTable diskSSTable = tablet.getDiskSSTable();

    int memSize = memorySSTable.getTabletSize(); 

    Map<String, TabletMapFile> mapFiles = null;
    String fileId = IdGenerator.getId();
    
    try {
      if(memSize == 0) {
        clearCommitLog();
        return;
      }
      LOG.debug(tablet.getTabletInfo().getTabletName() + ".compact(), dataCount:" + memorySSTable.getDataCount()[0] + ", memorySize:" + memSize);
      
      try {
        //1.MemTable freeze
        memorySSTable.initMemoryForCompaction();
    
        //2.save to temp dir
        //columnName, TabletMapFile      
        mapFiles = memorySSTable.saveToDisk(tabletInfo, fileId);
        
      } catch (Exception e) {
        LOG.error("MinorCompaction Error:" + e.getMessage() + "," + tabletInfo.getTabletName(), e);
        memorySSTable.cancelCompaction(fileId);
        
        return;
      } 
      
      try {
        //3.move map file from temp dir to table data dir
        for(Map.Entry<String, TabletMapFile> entry: mapFiles.entrySet()) {
          String columnName = entry.getKey();
          TabletMapFile mapFile = entry.getValue();
          moveMapFileFromTemp(diskSSTable, columnName, mapFile);  
        }
        
        //4. Column Memory Cache
        //diskSSTable.setMemoryCache(memorySSTable.getCompactingColumnCollections());
      } catch (Exception e) {
        LOG.fatal("TabletServer shutdowned cause fail to move mapFile from temp after minorCompaction," + tabletInfo.getTabletName(), e);
        tablet.getTabletServer().shutdown();
        return;
      }
      
      try {
        memorySSTable.clearCompactingColumnCollections(); 

        //temp dir 삭제
        GPath tempPath = Tablet.getTabletMinorCompactionTempPath(tablet.getConf(), tabletInfo);
        try {
          CloudataFileSystem fs = CloudataFileSystem.get(tablet.getConf());
          fs.delete(tempPath, true);
        } catch (IOException e) {
          LOG.error("Can't minor compaction temp dir:" + tempPath, e);
        }
      } catch(Exception e) {
        LOG.fatal("TabletServer shutdowned cause fail to delete CommitLogFile after minorCompaction:" + tabletInfo.getTabletName(), e);
        tablet.getTabletServer().shutdown(); 
        return;
      }
      
      //정상적으로 처리되고 forced = true인 경우에는 commit log를 reset
      clearCommitLog();
    } finally {
      try {
        // by sangchul
        // If the tablet is in the mode of clearing commitlog error, 
        // this two methods are important to do so.
        
        // this method asks commitLogClient to remove stale commitlog location info
        // and make next commitment elect new locations of commitlog servers
        memorySSTable.endCommitLogMinorCompaction();
        
        // this method permits to accept next apply operations from clients
        tablet.clearCommitLogErrorState();
      } catch (Exception e) {
        LOG.error("MinorCompaction finally error:" + tabletInfo.getTabletName(), e);
        throw new IOException(e.getMessage(), e);
      } finally {
        tablet.getActionChecker().endAction(this);
        
        long elapsedTime = System.currentTimeMillis() - startTime;
        
        if(elapsedTime > 0) {
          tablet.setMinorCompactionTime(memSize / elapsedTime);
        }
        
        LOG.info("TabletMinorCompaction End: " + tablet.getTabletInfo().getTabletName() 
            + ", mapFileCount=" + diskSSTable.getMaxMapFileCount()
            + ",time=" + elapsedTime);
        tablet.getTabletServer().tabletServerMetrics.setMinorCompactionTime(elapsedTime);        
      }
    }
  }
  
  private void clearCommitLog() {
    try {
      if(forced) {
        tablet.getMemorySSTable().clearCommitLog();
      }
    } catch (Exception e) {
      LOG.warn("Error clearCommit" +
      		"Log while MinorCompaction:" + tabletInfo + ":" + e.getMessage(), e);
    }  
  }
  public void moveMapFileFromTemp(DiskSSTable diskSSTable, String columnName, TabletMapFile mapFile ) throws IOException {
    GPath targetPath = new GPath(Tablet.getTabletPath(tablet.getConf(), 
        tablet.getTabletInfo()), 
        columnName + "/" + mapFile.getFileId() + "/");
    mapFile.moveTo(targetPath, true);
    diskSSTable.addTabletMapFile(columnName, mapFile);
  }
  
  public String getActionKey() {
    return tablet == null ? "null:" + getActionType() : tablet.getTabletInfo().getTabletName() + getActionType();
  }

  public List<String> getRunCheckActionTypes() {
    return checkActions;
  }
  
  public List<String> getWaitCheckActionTypes() {
    List<String> actions = new ArrayList<String>();
    actions.add(TabletSplitFinishAction.ACTION_TYPE);
    actions.add(TabletSplitAction.ACTION_TYPE);
    return actions;
  }

  @Override
  public String getActionType() {
    return ACTION_TYPE;
  }

  public String getTestHandlerKey() {
    return tablet.getTabletServer().getHostName();
  }
  
  public TabletServer getTabletServer() {
    return tablet.getTabletServer();
  }
}
