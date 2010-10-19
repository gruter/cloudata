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
import org.cloudata.core.client.Row;
import org.cloudata.core.common.IdGenerator;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.MemorySSTable;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.TabletServer;


/**
 * 1. lock put
 * 2. find median row key
 * 3. make empty two Tablets
 * 4. split memory
 * 5. release put lock
 * (put request add old Tablet and splited Tablet)
 * 6. split disk
 * 7. save to disk splited Tablet's MemorySSTable
 * (can put while doing 6 ~ 8 operation)
 * 8. call FinishAction
 * @author jindolk
 *
 */
public class TabletSplitAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(TabletSplitAction.class.getName());
  public static final String ACTION_TYPE = "TabletSplitAction";
  
  static List<String> checkActionTypes = new ArrayList<String>();
  static List<String> checkWaitingActionTypes = new ArrayList<String>();
  static List<String> checkActionTypesForFinish = new ArrayList<String>();

  static {  
    checkActionTypes.add(MinorCompactionAction.ACTION_TYPE);
    checkActionTypes.add(MajorCompactionAction.ACTION_TYPE);
    checkActionTypes.add(TabletSplitAction.ACTION_TYPE);
    checkActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
    checkActionTypes.add(TabletDropAction.ACTION_TYPE);
    checkActionTypes.add(BatchUploadAction.ACTION_TYPE);
    checkActionTypes.add(TabletLockAction.ACTION_TYPE);
    
    checkWaitingActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);

    checkActionTypesForFinish.add(ScannerAction.ACTION_TYPE);
    checkActionTypesForFinish.add(CommitAction.ACTION_TYPE);
    checkActionTypesForFinish.add(ApplyAction.ACTION_TYPE);
    checkActionTypesForFinish.add(FlushAction.ACTION_TYPE);
    checkActionTypesForFinish.add(TabletDropAction.ACTION_TYPE);
    checkActionTypesForFinish.add(TxAction.ACTION_TYPE);
    checkActionTypesForFinish.add(TabletLockAction.ACTION_TYPE);
  }

  private Tablet tablet;
  private TabletServer tabletServer;
  
  private Tablet[] splitedTablets;
  private TabletInfo[] splitedTabletInfos;
  private Row.Key midRowKey;
  private boolean end = true;
  private CloudataFileSystem fs; 
  private Object monitor = new Object();
  
  private boolean singleRowTablet = false;
  
  public TabletSplitAction(Tablet tablet, TabletServer tabletServer) {
    this.tablet = tablet;
    this.tabletServer = tabletServer;
    if(tablet != null) {
      this.fs = CloudataFileSystem.get(tablet.getConf());
    }
  }
  
  public boolean isThreadMode() {
    return true;
  }

  public boolean isWaitingMode(ActionChecker actionChecker) {
    return !(actionChecker.runningActions.contains(TabletSplitAction.ACTION_TYPE) ||
        actionChecker.runningActions.contains(TabletSplitFinishAction.ACTION_TYPE) ||
        actionChecker.waitingActions.contains(TabletSplitAction.ACTION_TYPE) ||
        actionChecker.waitingActions.contains(TabletSplitFinishAction.ACTION_TYPE));
  }
  
  public void run() {
    synchronized(monitor) {
      if(singleRowTablet) {
        return;
      }
  
      long startTime = System.currentTimeMillis();
      TabletInfo tabletInfo = tablet.getTabletInfo();
      LOG.info("TabletSplitAction start: " + tabletInfo);
      
      tablet.startSplit();
      try {
        //0. save memory to disk
        //saveMemoryToDisk(tablet);
        
        //1. lock put
        //LOG.info("Put lock for split:" + tabletInfo.getTabletName());
        long lockStartTime = System.currentTimeMillis();
        long lockTime = 0;
        tablet.setApplyLock(true);
        tablet.blockLatch();
        try {
          //2. find median row key
          //3. make empty two Tablets
          this.splitedTabletInfos = makeSplitTabletInfo(tabletInfo);
          if(splitedTabletInfos == null) {
            singleRowTablet = true;
            LOG.warn("Can't find mid rowkey:" + tabletInfo);
            return;
          }
  
          if(tabletInfo.getEndRowKey().equals(splitedTabletInfos[0].getEndRowKey()) ||
              splitedTabletInfos[0].getEndRowKey().equals(splitedTabletInfos[1])) {
            singleRowTablet = true;
            LOG.warn("Splited Tablet has same end rowkey:" + tabletInfo + "," + 
                splitedTabletInfos[0] + "," +
                splitedTabletInfos[1]);
            return;
          }
    
          this.splitedTablets = new Tablet[2];
          splitedTablets[0] = new Tablet(tablet.getConf(), tablet.getTable(), tabletServer, splitedTabletInfos[0]);
          splitedTablets[1] = new Tablet(tablet.getConf(), tablet.getTable(), tabletServer, splitedTabletInfos[1]);

          splitedTablets[0].blockLatch();
          splitedTablets[1].blockLatch();
          
          //4. split memory
          tablet.getMemorySSTable().split(midRowKey, splitedTablets);
          
          tablet.setSplitingTablets(this.splitedTablets);
          //여기까지 처리에서 에러가 발생한 경우에는 아무런 처리를 해줄 필요 없다.
        } finally {
          //5. release put lock
          tablet.unblockLatch();
          for(int i = 0; i < 2; i++) {
            if(splitedTablets[i] != null) {
              splitedTablets[i].unblockLatch();
              splitedTablets[i].getReady().compareAndSet(false, true);
            }
          }
          tablet.setApplyLock(false);
          lockTime = System.currentTimeMillis() - lockStartTime;
        }
        //LOG.info("Release put lock for split:" + tabletInfo.getTabletName());
        
        //6. split disk
        tablet.getDiskSSTable().split(midRowKey, splitedTablets);

        //7. save to disk splited Tablet's MemorySSTable
        saveMemoryToDisk(splitedTablets[0]);
        saveMemoryToDisk(splitedTablets[1]);
        
        splitedTablets[0].getReady().set(false);
        splitedTablets[1].getReady().set(false);
        
        //splitedTablets[1].getDiskSSTable().loadToMemoryCache();
        
        //8. call FinishAction        
        long endTime = System.currentTimeMillis();
        LOG.info("TabletSplitAction end: " + tabletInfo + "," + 
            splitedTabletInfos[0] + "," + splitedTabletInfos[1] + "," +
        		"time:" + (endTime - startTime) + ", lockTime:" + lockTime);
        //파일의 분리 작업이 완료되면 메모리의 내용도 분리한다.
        //메모리의 내용을 분리하기 전에 Tablet에서 다른 처리를 하고 있으면 기다린다.(따라서 다른 Action으로 분리하여 처리)
        TabletSplitFinishAction tabletSplitFinishAction = new TabletSplitFinishAction(this);
        tablet.getActionChecker().startAction(tabletSplitFinishAction);
        
        tablet.getTabletServer().tabletServerMetrics.setSplitTime(endTime - startTime);   
      } catch (Exception e) {
        LOG.error("split run error:" + e.getMessage(), e);
        //장애 발생시 Temp 내용 삭제
        rollback();
      } finally {
        tablet.getActionChecker().endAction(this);
        end = true;
        tablet.endSplit();
      }
    }
  }

  private void saveMemoryToDisk(Tablet targetTablet) throws IOException {
    MemorySSTable memorySSTable = targetTablet.getMemorySSTable();
    Map<String, TabletMapFile> mapFiles = null;
    String fileId = IdGenerator.getId();
    try {
      //1.MemTable freeze
      memorySSTable.initMemoryForCompaction();
  
      //2.save to temp dir
      //columnName, TabletMapFile      
      mapFiles = memorySSTable.saveToDisk(targetTablet.getTabletInfo(), fileId);

      //3.add mapfile
      for(Map.Entry<String, TabletMapFile> entry: mapFiles.entrySet()) {
        String columnName = entry.getKey();
        TabletMapFile mapFile = entry.getValue();
        
        targetTablet.getDiskSSTable().addTabletMapFile(columnName, mapFile);
      }

      memorySSTable.clearCompactingColumnCollections(); 
    } catch(Exception e) {
      LOG.error("saveMemoryToDisk Error:" + e.getMessage() + "," + targetTablet.getTabletInfo().getTabletName(), e);
      throw new IOException(e);
    }
  }
  
  private void rollback() {
    if(this.splitedTabletInfos != null) {
      for(int i = 0; i < this.splitedTabletInfos.length; i++) {
        try {
          GPath tempPath = Tablet.getTabletSplitTempPath(tablet.getConf(), splitedTabletInfos[i]);
          fs.delete(tempPath, true);
          LOG.error("split run error: clear split temp dir:" + tempPath);
        } catch (Exception e) {
          LOG.error("Error while split rollback:" + e.getMessage(), e);
        }
      }
      tablet.setSplitingTablets(null);
    }
    tablet.getReady().set(true);
  }
  
  private TabletInfo[] makeSplitTabletInfo(TabletInfo tabletInfo) throws IOException {
    TabletInfo[] splitedTabletInfos = new TabletInfo[2];
    splitedTabletInfos[0] = new TabletInfo(tabletInfo.getTableName(), 
        Tablet.generateTabletName(tabletInfo.getTableName()),
        Row.Key.MIN_KEY,
        Row.Key.MIN_KEY);
    splitedTabletInfos[1] = new TabletInfo(tabletInfo.getTableName(), 
        Tablet.generateTabletName(tabletInfo.getTableName()),
        Row.Key.MIN_KEY,
        Row.Key.MIN_KEY);
    
    //첫번째 Tablet은 다른 Tablet으로 할당, 두번째 Tablet은 자신이 서비스
    splitedTabletInfos[1].setAssignedHostName(tabletInfo.getAssignedHostName());
    
    this.midRowKey = tablet.getDiskSSTable().findMidRowKeyForSplit();
    //midRow.Key가 null인 경우는 diskSSTable에 MapFile이 하나 이상인 경우
    //이 경우에는 다음 MajorCompaction 수행 후 처리된다.
    if(midRowKey == null) {
      midRowKey = tablet.getMemorySSTable().findMidRowKeyForSplit();
      if(midRowKey == null) {
        return null;
      }
    }
    splitedTabletInfos[0].setStartRowKey(tabletInfo.getStartRowKey());
    splitedTabletInfos[0].setEndRowKey(midRowKey);
 
    splitedTabletInfos[1].setStartRowKey(midRowKey);
    splitedTabletInfos[1].setEndRowKey(tabletInfo.getEndRowKey());
    return splitedTabletInfos;
  }
  
  public void setEnd(boolean end) {
    this.end = end;
  }
  
  public boolean isEnd() {
    return end;
  }
  
  public Tablet[] getSplitedTablets() {
    return splitedTablets;
  }
  
  class TabletSplitFinishAction extends TabletAction { 
    public static final String ACTION_TYPE = "TabletSplitFinishAction";
    
    private TabletSplitAction tabletSplitAction;
    
    public TabletSplitFinishAction(TabletSplitAction tabletSplitAction) {
      this.tabletSplitAction = tabletSplitAction;
    }
    
    public boolean isThreadMode() {
      return true;
    }

    public boolean isWaitingMode(ActionChecker actionChecker) {
      return true;
    }
    
    public void run() {
      long startTime = System.currentTimeMillis();
      try {
        tablet.startSplit();
        tablet.blockLatch();
        if(!tablet.isReady() || tablet.isDroped()) {
          LOG.debug("TabletSplitFinishAction stoped:ready=" + tablet.isReady() + ", droped=" + tablet.isDroped());
          return;
        }
        tablet.getReady().compareAndSet(true, false);
        TabletInfo tabletInfo = tablet.getTabletInfo();
        LOG.info("TabletSplitFinishAction start: " + tabletInfo);

        //Tablet 파일 Move
        try {
          splitedTablets[0].getDiskSSTable().moveTempToTablet(false);
          splitedTablets[1].getDiskSSTable().moveTempToTablet(false);
        } catch (Exception e) {
          //오류 발생 시 temp dir 삭제
          LOG.error("moveTempToTablet error:" + tabletInfo, e);
          rollback();
          
          //move된 tablet 파일 삭제
          for(int i = 0; i < 2; i++) {
            try {
              GPath dataPath = Tablet.getTabletPath(tablet.getConf(), splitedTablets[i].getTabletInfo());
              fs.delete(dataPath, true);
            } catch (Exception e1) {
              LOG.warn("Exception in deleting tablet file", e1);
            }
          }
          return;
        }
        
        //TabletServer에 등록 & Meta Data에 등록
        try {
          if(tabletServer != null) {
            tabletServer.tabletSplited(tablet, splitedTablets);
          }
          splitedTablets[0].getDiskSSTable().calculateTotalMapFileSize();
          splitedTablets[1].getDiskSSTable().calculateTotalMapFileSize();
        } catch (Exception e) {
          //META 데이터 복구는 tabletServer의 tabletSplited에서 수행 -> 복구 필요 없음(3개의 작업을 1tx에서 처리)
          //temp dir 삭제
          LOG.error("modify meta info error: " + tabletInfo, e);
          rollback(); 
          tablet.getMemorySSTable().cancelCompaction(null);
          //move된 tablet 파일 삭제
          for(int i = 0; i < 2; i++) {
            try {
              GPath dataPath = Tablet.getTabletPath(tablet.getConf(), splitedTablets[i].getTabletInfo());
              fs.delete(dataPath, true);
            } catch (Exception e1) {
              e1.printStackTrace();
            }
          }   
          
          return;
        }
        
        try {
          //기존 Tablet Data 삭제
          tablet.deleteTablet();
        } catch (Exception e) {
          //여기서 오류가 발생해도 아무런 처리를 하지 않아도 됨
          //TODO 나중에 별도로 전체적으로 clean 작업해주는 필요 
          LOG.error("old tablet drop error", e);
        }
        
        //temp file 삭제
        try {
          splitedTablets[0].deleteSplitTempDir();
          splitedTablets[1].deleteSplitTempDir();
        } catch (Exception e) {
          //여기서 오류가 발생해도 아무런 처리를 하지 않아도 됨
          //TODO 나중에 별도로 전체적으로 clean 작업해주는 필요 
          LOG.error("deleteSplitTempDir", e);
        }
        
        //split된 tablet 서비스 준비 완료 설정
        splitedTablets[1].getReady().compareAndSet(false, true);

        LOG.info("TabletSplitFinishAction end: " + tabletInfo + "," + 
            splitedTabletInfos[0] + ", " + splitedTabletInfos[1] + ", time: " + (System.currentTimeMillis() - startTime));
      } catch (Exception e) {
        LOG.error("run error", e);
      } finally {
        try {
          tablet.getActionChecker().endAction(this);
          tablet.getActionChecker().endAction(tabletSplitAction);
          end = true;
          tablet.getTabletServer().tabletServerMetrics.setSplitFinishTime(System.currentTimeMillis() - startTime);
        } catch (Exception e) {
          LOG.error(e);
        }
        tablet.endSplit();
        tablet.unblockLatch();
      }
    }

    @Override
    public String getActionKey() {
      return tablet == null ? "null:" + getActionType() : tablet.getTabletInfo().getTabletName() + getActionType();
      //return getActionType();
    }

    @Override
    public String getActionType() {
      return ACTION_TYPE;
    }

    @Override
    public List<String> getRunCheckActionTypes() {
      return checkActionTypesForFinish;
    }
  }

  @Override
  public String getActionKey() {
    return tablet == null ? "null:" + getActionType() : tablet.getTabletInfo().getTabletName() + getActionType();

    //return getActionType();
  }

  public TabletSplitFinishAction getTabletSplitFinishAction() {
    return new TabletSplitFinishAction(this);
  }

  @Override
  public String getActionType() {
    return ACTION_TYPE;
  }

  @Override
  public List<String> getRunCheckActionTypes() {
    return checkActionTypes;
  }
  
  public List<String> getWaitCheckActionTypes() {
    return checkWaitingActionTypes;
  }
}
