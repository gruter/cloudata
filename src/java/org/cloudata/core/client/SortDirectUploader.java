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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.AsyncTaskStatus;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.UploaderCache;


public class SortDirectUploader extends DirectUploader {
  private static final Log LOG = LogFactory.getLog(SortDirectUploader.class.getName());
  
  private Row.Key previousTabletEndRowKey;
  
  private UploaderCache batchUploadCache;
  
  //uploadActionId -> UploadedMapFiles
  private Map<String, UploadTarget> uploadTargets = new HashMap<String, UploadTarget>();
  
  private int uploaderCount = 0;
  
  private String uploadActionId;
  
  protected SortDirectUploader(CTable ctable, String[] columnNames) throws IOException {
    super(ctable, columnNames);
    if(ctable.getConf().get("cloudata.local.temp") == null) {
      throw new IOException("No config variable:cloudata.local.temp");
    }
  }
  
  protected TabletInfo findNextTablet(Row.Key rowKey) throws IOException {
    if(rowKey == null) {
      return null;
    }
    
    if(tabletInfos == null) {
      tabletInfos = ctable.listTabletInfos();
    }
    
    previousTabletEndRowKey = Row.Key.MIN_KEY;
    
    for(TabletInfo tabletInfo: tabletInfos) {
      if(tabletInfo.getEndRowKey().compareTo(rowKey) >= 0) {
        return tabletInfo;
      }
      previousTabletEndRowKey = tabletInfo.getEndRowKey();
    }
    return null;
  }
  
  protected boolean belongRowKey(Row.Key rowKey) throws IOException {
    if(previousTabletEndRowKey == null) {
      return false;
    }
    
    boolean result = rowKey.compareTo(previousTabletEndRowKey) > 0 && 
    rowKey.compareTo(currentTabletInfo.getEndRowKey()) <= 0;
    
    return result;
  }
  
  /**
   * 처음 호출되거나 입력된 rowKey가 현재의 TabletInfo의 row 범위를 벗어나는 경우
   * 새로운 TabletInfo 정보를 가져온다. 
   * @param rowKey
   * @throws IOException
   */
  private void connectUploader(Row.Key rowKey) throws IOException {
    if(currentTabletInfo == null || !belongRowKey(rowKey)) {
      internalClose();
      currentTabletInfo = findNextTablet(rowKey);
      
      if(currentTabletInfo == null) {
        throw new IOException("Can't find tablet (rowkey=" + rowKey + ")");
      }
      //LOG.info("connectBatchUploader Start:" + rowKey + ", " + Row.Key.hashValue(rowKey.getKey()) + "," + currentTabletInfo);
      DataServiceProtocol tabletServer = CTableManager.connectTabletServer(currentTabletInfo.getAssignedHostName(), ctable.getConf());
      uploadActionId = tabletServer.startBatchUploader(currentTabletInfo.getTabletName(), rowKey, false);
      if(uploadActionId == null) {
        throw new IOException("Fail cause tablet not allowed opening uploader: " + currentTabletInfo);
      }
      
      batchUploadCache = new UploaderCache(ctable.getConf(), currentTabletInfo, columnNames, uploadActionId, ctable.descTable().getNumOfVersion());
      uploaderCount++;
      LOG.info(rowKey + " connection to DirectUploader:" + currentTabletInfo);
    }
  }
  
  @Override
  public void put(Row row) throws IOException {
    if(row == null || row.getColumnCount() == 0) {
      return;
    }

    int retry = 0;
    int maxRetry = 20;
    String message = "No error";

    while(retry < maxRetry) {
      try {
        connectUploader(row.getKey());
        break;
      } catch (Exception e) {
        e.printStackTrace(System.out);
        retry++;
        tabletInfos = null;
        currentTabletInfo = null;
        batchUploadCache = null;
        message = e.getMessage();
        LOG.info("connectBatchUploader retry cause by: " + e.getMessage() + ",rowKey=" + row.getKey(), e);
      }
      try {
        Thread.sleep(1 * 1000);
      } catch (InterruptedException e) {
      }
    }
    
    if(retry >= maxRetry) {
      close();
      throw new IOException("Timeout after " + maxRetry + " retrys (rowKey=" + row.getKey() + ", message=" + message + ")");
    }
    
    try {
      ColumnValue[][] columnValues = rowToColumnValue(row);
      if(columnValues == null) {
        return;
      }
      batchUploadCache.addColumnRecords(row.getKey(), columnValues);
    } catch (IOException e) {
      e.printStackTrace(System.out);
      //FIXME 오류 발생시 처리 방법.
      throw e;
    }
  }
  
  @Override
  public void rollback() throws IOException {
    LOG.info("all temp files removed:uploadTargets.size()=" + uploadTargets.size());
    
    for(UploadTarget uploadTarget: uploadTargets.values()) {
      for(int i = 0; i < columnNames.length; i++) {
        TabletMapFile mapFile = uploadTarget.tabletMapFiles.get(columnNames[i]);
        if(mapFile == null) {
          continue;
        }
        
        try {
          mapFile.delete();
        } catch (Exception e) {
          LOG.error(e);
        }
      }
      String path = ctable.getConf().get("cloudata.local.temp") + "/" + uploadTarget.uploadActionId;
      LOG.info("deleting local temp dir:" + path);
      if(!FileUtil.delete(path, true)) {
        try {
          Thread.sleep(2*1000);
        } catch (InterruptedException e) {
          return;
        }
        if(!FileUtil.delete(path, true)) {
          LOG.warn("Can't delete local temp file for uploading:" + path);
        }
      } 
      //call cancelBatchUploader()
      try {
        LOG.info("Call cancelBatchUploader:" + uploadTarget.tabletInfo);
        DataServiceProtocol tabletServer = CTableManager.connectTabletServer(uploadTarget.tabletInfo, ctable.getConf());
        tabletServer.cancelBatchUploader(uploadTarget.uploadActionId,
            uploadTarget.tabletInfo.getTabletName());
      } catch (IOException e) {
        LOG.error("Can't cancel batch uploader:" + uploadTarget.tabletInfo, e);
      }      
    }
    
    if(batchUploadCache != null && currentTabletInfo != null) {
      try {
        LOG.info("Call cancelBatchUploader:" + currentTabletInfo);
        DataServiceProtocol tabletServer = CTableManager.connectTabletServer(currentTabletInfo, ctable.getConf());
        tabletServer.cancelBatchUploader(uploadActionId,
            currentTabletInfo.getTabletName());
      } catch (IOException e) {
        LOG.error("Can't cancel batch uploader:" + currentTabletInfo, e);
      }      
    }
    
    uploadTargets.clear();
    batchUploadCache = null;
    currentTabletInfo = null;
    tabletInfos = null;
  }
  
  @Override
  public synchronized void close() throws IOException {
    LOG.info("Started Uploader closing:" + ctable.getTableName() + "," + columnNames[0]);
    try {
      internalClose();
      
      LOG.info("End internalClose:" + ctable.getTableName() + "," + columnNames[0]);
      //send map file info to tablet server
      for(UploadTarget eachUploadTarget: uploadTargets.values()) {
        String[] mapFileIds = new String[columnNames.length];
        String[] mapFilePaths = new String[columnNames.length];
  
        for(int i = 0; i < columnNames.length; i++) {
          TabletMapFile mapFile = eachUploadTarget.tabletMapFiles.get(columnNames[i]);
          
          if(mapFile == null) {
            mapFileIds[i] = null;
            mapFilePaths[i] = null;
          } else {
            mapFileIds[i] = mapFile.getFileId();
            mapFilePaths[i] = mapFile.getFilePath().toString();
          }
        }
        
        //END 처리는 TabletServer와 async 하게 처리한다.
        //먼저 endBatchUploader를 요청하고 처리 상태를 계속 모니터링 한다.
        TabletInfo liveTabletInfo = eachUploadTarget.tabletInfo;
        String taskId = null;
        int retry = 0;
        Exception exception = null;
        DataServiceProtocol tabletServer = null;
        while(retry < 10) {
          try {
            LOG.info("Call endBatchUploader:" + liveTabletInfo);
            tabletServer = CTableManager.connectTabletServer(liveTabletInfo, ctable.getConf());
            taskId = tabletServer.endBatchUploader(eachUploadTarget.uploadActionId,
                eachUploadTarget.tabletInfo.getTabletName(), 
                columnNames, mapFileIds, mapFilePaths);
            if(taskId != null) {
              break;
            }
          } catch (Exception e) {
            LOG.error("Error tabletServer.endBatchUploader cause:" + e.getMessage() + ". but retry(" + retry + ")");
            exception = e;
          }
          retry++;
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e1) {
            return;
          }
          if(liveTabletInfo != null) {
            //local cache를 clear한다.
            ctable.locationCache.clearTabletCache(
                eachUploadTarget.tabletInfo.getTableName(), 
                eachUploadTarget.tabletInfo.getEndRowKey(), 
                liveTabletInfo);
          }
          TabletInfo previousTabletInfo = liveTabletInfo;
          //tablet 정보를 다시 가져온다.(여기까지 온 경우 Tablet이 변경되었을 가능성이 많음)
          ctable.locationCache.clearTabletCache(eachUploadTarget.tabletInfo.getTableName(), eachUploadTarget.tabletInfo.getEndRowKey(), liveTabletInfo);
          liveTabletInfo = ctable.lookupTargetTablet(eachUploadTarget.tabletInfo.getEndRowKey());
          LOG.info("retry tabletServer.endBatchUploader, change tableinfo from [" + previousTabletInfo + "] to [" + liveTabletInfo + "]");
        }
        
        //에러가 발생한 경우
        if(retry >= 10 && exception != null) {
          try {
            LOG.error("error while endBatchUploader cause:" + exception.getMessage(), exception);
            rollback();
          } catch (Exception e) {
            LOG.error(e);
          }
          IOException err = new IOException(exception.getMessage());
          err.initCause(exception);
          throw err;
        }
        
        //정상 요청된 경우
        long startTime = System.currentTimeMillis();
        Row.Key endRowKey = liveTabletInfo.getEndRowKey();
        String tabletName = liveTabletInfo.getTabletName();
        
        LOG.info("Check TabletServer ending status:" + tabletName + "," + columnNames[0]);
        while(true) {
          //FIXME 완료 요청을 받지 못한 경우 처리는 어떻게 하나?
          //현재는 WARN만 준다.
          try {
            AsyncTaskStatus task = tabletServer.getAsyncTaskStatus(taskId);
            if(task != null) {
              if(task.isEnd()) {
                if(task.isError()) {
                  //에러 처리
                  rollback();
                  throw new IOException(task.getErrorTrace());
                } else {
                  break;
                }
              }
            } else { 
              //task == null
              LOG.warn("No Upload closing response from tablerserer(" + liveTabletInfo.getAssignedHostName() + "). " + 
                  "tablet=" + liveTabletInfo.getTabletName());
              //다른 TabletServer로 한번 더 시도한다.
              try {
                Thread.sleep(5 * 1000);
                if(liveTabletInfo != null) {
                  ctable.locationCache.clearTabletCache(liveTabletInfo.getTableName(), 
                      endRowKey, liveTabletInfo);
                }
                TabletInfo lookupedTabletInfo = ctable.lookupTargetTablet(endRowKey);
                if(lookupedTabletInfo != null) {
                  //Tablet의 host가 변경된 경우
                  if(!lookupedTabletInfo.getAssignedHostName().equals(liveTabletInfo.getAssignedHostName())) {
                    tabletServer = CTableManager.connectTabletServer(lookupedTabletInfo, ctable.getConf());
                    taskId = tabletServer.endBatchUploader(
                        eachUploadTarget.uploadActionId,
                        tabletName, 
                        columnNames, mapFileIds, mapFilePaths);
                    liveTabletInfo = lookupedTabletInfo;
                  }
                }
              } catch (Exception e) {
                LOG.error("Error while retrying endBatchUploader(" + liveTabletInfo.getAssignedHostName() + "). " + 
                    "tablet=" + liveTabletInfo.getTabletName(), e);
              }            
            }
          } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            try {
              Thread.sleep(5 * 1000);
              if(liveTabletInfo != null) {
                ctable.locationCache.clearTabletCache(eachUploadTarget.tabletInfo.getTableName(), 
                    eachUploadTarget.tabletInfo.getEndRowKey(), liveTabletInfo);
              }
              TabletInfo lookupedTabletInfo = ctable.lookupTargetTablet(endRowKey);
              if(lookupedTabletInfo != null) {
                //Tablet의 host가 변경된 경우
                if(!lookupedTabletInfo.getAssignedHostName().equals(liveTabletInfo.getAssignedHostName())) {
                  tabletServer = CTableManager.connectTabletServer(lookupedTabletInfo, ctable.getConf());
                  taskId = tabletServer.endBatchUploader(
                      eachUploadTarget.uploadActionId,
                      tabletName, 
                      columnNames, mapFileIds, mapFilePaths);
                  liveTabletInfo = lookupedTabletInfo;
                }
              }
            } catch (Exception err) {
              LOG.error("Error while retrying endBatchUploader(" + liveTabletInfo.getAssignedHostName() + "). " + 
                  "tablet=" + liveTabletInfo.getTabletName(), err);
            }               
          }

          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            return;
          }
          
          if(System.currentTimeMillis() - startTime > 180 * 1000) {
            //FIXME 이 경우 서버에서는 계속 처리하고 있을 가능성이 있다.
            //어떻게 해야 하나?
            LOG.error("Timeout while endBatchUploader(" + (System.currentTimeMillis() - startTime) + " ms)" + liveTabletInfo);
            throw new IOException("Timeout while endBatchUploader(" + (System.currentTimeMillis() - startTime) + " ms)" + liveTabletInfo);
          }
        }
      } //for each UploadTarget   
    } finally {
      for(UploadTarget uploadTarget: uploadTargets.values()) {
        String path = ctable.getConf().get("cloudata.local.temp") + "/" + uploadTarget.uploadActionId;
        LOG.info("deleting local temp dir:" + path);
        if(!FileUtil.delete(path, true)) {
          try {
            Thread.sleep(2*1000);
          } catch (InterruptedException e) {
            return;
          }
          if(!FileUtil.delete(path, true)) {
            LOG.warn("Can't delete local temp file for uploading:" + path);
          }
        }
      }
      LOG.info("Ended Uploader closing");
    }
  }
  
  private void cancelUpload() throws IOException {
    //TODO 
  }
  
  private synchronized void internalClose() throws IOException {
    try {
      //save to map file
      if(batchUploadCache != null) {
        Map<String, TabletMapFile> mapFiles = batchUploadCache.close();
        
        if(mapFiles != null) {
          UploadTarget uploadTarget = new UploadTarget(uploadActionId, 
              currentTabletInfo, 
              mapFiles);
          
          uploadTargets.put(uploadActionId, uploadTarget);
        }
      } 
    } catch (Exception e) {
      e.printStackTrace(System.out);
      try {
        rollback();
      } catch (Exception err) {
        LOG.error(err);
      }
      throw new IOException(e.getMessage());
    } finally {
      batchUploadCache = null;
      currentTabletInfo = null;
    }
  }
    
  class UploadTarget {
    String uploadActionId;
    TabletInfo tabletInfo;
    //ColumnName -> MapFiles
    Map<String, TabletMapFile> tabletMapFiles = 
      new HashMap<String, TabletMapFile>();
    
    public UploadTarget(String uploadActionId, TabletInfo tabletInfo, 
        Map<String, TabletMapFile> pTabletMapFiles) {
      this.uploadActionId = uploadActionId;
      this.tabletInfo = tabletInfo;
      
      if(pTabletMapFiles == null) {
        return;
      }
      
      for(Map.Entry<String, TabletMapFile> entry: pTabletMapFiles.entrySet()) {
        String columnName = entry.getKey();
        
        TabletMapFile mapFile = entry.getValue();
        tabletMapFiles.put(columnName, mapFile);
        //System.out.println("UploadedMapFile>>>>>>>>>>>>>>>>>" + tabletInfo.getTabletName() + ">" + entry.getValue().size() + ">" + mapFiles.size());
      }
    }
    
//    public void print() {
//      for(Map.Entry<String, List<TabletMapFile>> entry: tabletMapFiles.entrySet()) {
//        for(TabletMapFile eachMapFile: entry.getValue()) {
//          System.out.println(DFSBatchUploader.this + ":" + tabletInfo.getTabletName() + ":" + eachMapFile.getFileId() + ">" + uploadActionId);
//        }
//      }
//    }
  }
  
  public static void main(String[] args) throws IOException {
    String[] columnNames = new String[]{"item", "z", "dummy"};
    Row.Key rowKey = new Row.Key("aaa");
    
    Row row = new Row(rowKey);
    for(int i = 0; i < 10; i++) {
      row.addCell("item", new Cell(new Cell.Key(("" + i)), ("" + i).getBytes()));
    }

    for(int i = 0; i < 5; i++) {
      row.addCell("z", new Cell(new Cell.Key(("" + i)), ("" + i).getBytes()));
    }

    row.addCell("dummy", new Cell(Cell.Key.EMPTY_KEY, null));
    
    try {
      ColumnValue[][] columnValues = new ColumnValue[row.getColumnCount()][]; 
      
      int count = 0;
      
      int index = 0;
      for(String eachColumn: columnNames) {
        List<Cell> cells = row.getCellList(eachColumn);
        List<ColumnValue> columnValueList = new ArrayList<ColumnValue>();
        if(cells == null || cells.size() == 0) {
          continue;
        }
        
        for(Cell eachCell: cells) {
          for(Cell.Value eachValue: eachCell.getValues()) {
            ColumnValue columnValue = new ColumnValue();
            columnValue.setRowKey(row.getKey());
            columnValue.setCellKey(eachCell.getKey());
            eachValue.copyToColumnValue(columnValue);
            columnValueList.add(columnValue);
            count++;
          }
        }
        
        columnValues[index] = columnValueList.toArray(new ColumnValue[]{});
        index++;
      }
      
      for(int i = 0; i < columnValues.length; i++) {
        System.out.println("----------------------------------");
        for(int j = 0; j < columnValues[i].length; j++) {
          System.out.println(columnValues[i][j]);
        }
      }
    } catch (Exception e) {
      e.printStackTrace(System.out);
    }
  }
}
