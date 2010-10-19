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
import java.util.HashMap;
import java.util.Map;

import org.cloudata.core.common.IdGenerator;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.AsyncTaskStatus;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileWriter;


/**
 * @author jindolk
 *
 */
public class DefaultDirectUploader extends DirectUploader {
  //columnNam -> MapFile
  private Map<String, TabletMapFile> mapFiles;

  private Map<String, MapFileWriter> mapFileWriters;

  private int uploaderCount = 0;
  
  private String uploadActionId;
  
  private Row.Key previousKey = Row.Key.MIN_KEY;
  
  private Thread leaseToucher;
  
  protected DefaultDirectUploader(CTable ctable, String[] columnNames) throws IOException {
    super(ctable, columnNames);
    previousTabletEndRowKey = Row.Key.MIN_KEY;
  }
  
  protected TabletInfo findNextTablet(Row.Key rowKey) throws IOException {
    if(rowKey == null) {
      return null;
    }
    
    TabletInfo tabletInfo = CTable.lookupTargetTablet(ctable.getConf(), ctable.getTableName(), rowKey);
    
    if(tabletInfo != null) {
      if(currentTabletInfo != null) {
        previousTabletEndRowKey = currentTabletInfo.getEndRowKey();
      }
    } 
    return tabletInfo;
  }
  
  protected boolean belongRowKey(Row.Key rowKey) throws IOException {
    if(previousTabletEndRowKey == null) {
      return false;
    }
    
    return rowKey.compareTo(previousTabletEndRowKey) > 0 && 
            rowKey.compareTo(currentTabletInfo.getEndRowKey()) <= 0;
  } 
  
  private void lookupTabletServer(Row.Key rowKey) throws IOException {
    if(currentTabletInfo == null || !belongRowKey(rowKey)) {
      internalClose();
      currentTabletInfo = findNextTablet(rowKey);
      
      if(currentTabletInfo == null) {
        throw new IOException("Can't find tablet (rowkey=" + rowKey + ")");
      }
      try {
        DataServiceProtocol tabletServer = CTableManager.connectTabletServer(currentTabletInfo.getAssignedHostName(), ctable.getConf());
        uploadActionId = tabletServer.startBatchUploader(currentTabletInfo.getTabletName(), rowKey, true);
        if(uploadActionId == null) {
          throw new IOException("Fail cause tablet not allowed opening uploader: " + currentTabletInfo);
        }
      } catch (IOException e) {
        ctable.locationCache.clearTabletCache(ctable.getTableName(), rowKey, currentTabletInfo);
      }
      
      mapFiles = new HashMap<String, TabletMapFile>();
      mapFileWriters = new HashMap<String, MapFileWriter>();
      
      String fileId = IdGenerator.getId() ;
      GPath resultParentPath = Tablet.getTabletUploadTempPath(ctable.getConf(), currentTabletInfo, uploadActionId);
      for(int i = 0; i < columnNames.length; i++) {
        GPath resultPath = new GPath(resultParentPath, columnNames[i] + "/" + fileId + "/");
        TabletMapFile tabletMapFile = new TabletMapFile(ctable.getConf(),
            CloudataFileSystem.get(ctable.getConf()),
            currentTabletInfo, columnNames[i], fileId, resultPath,
            ctable.descTable().getNumOfVersion());
        MapFileWriter mapFileWriter = tabletMapFile.getMapFileWriter();
        
        mapFiles.put(columnNames[i], tabletMapFile);
        mapFileWriters.put(columnNames[i], mapFileWriter);
      }
      
      leaseToucher = new Thread(new LeaseToucher(currentTabletInfo, ctable.getConf(),
          currentTabletInfo.getAssignedHostName(), uploadActionId));
      leaseToucher.setName("UploaderLeaseTouchThread_" + currentTabletInfo.getTabletName() + "_" + System.currentTimeMillis());
      leaseToucher.start();

      uploaderCount++;
      
      LOG.info(rowKey + " connection to DirectUploader:" + currentTabletInfo);
    }
  }

  private void lookupTabletServerRetry(Row.Key rowKey) throws IOException {
    int retry = 0;
    int maxRetry = 20;
    String message = "No error";

    while(retry < maxRetry) {
      try {
        lookupTabletServer(rowKey);
        break;
      } catch (Exception e) {
        retry++;
        tabletInfos = null;
        currentTabletInfo = null;
        message = e.getMessage();
        LOG.info("Error connectBatchUploader: " + e.getMessage() + ",rowKey=" + rowKey + ", but retry", e);
      }
      try {
        Thread.sleep(1 * 1000);
      } catch (InterruptedException e) {
      }
    }
    
    if(retry >= maxRetry) {
      close();
      throw new IOException("Timeout after " + maxRetry + " retrys (rowKey=" + rowKey + ", message=" + message + ")");
    }
  }
  
  @Override
  public void put(Row row) throws IOException {
    if(row == null || row.getColumnCount() == 0) {
      return;
    }

    if(row.getKey().compareTo(previousKey) < 0) {
      throw new IOException("Not sorted:" + row.getKey() + ", previous key:" + previousKey);
    }
    
    try {
      ColumnValue[][] columnValues = rowToColumnValue(row);
      if(columnValues == null) {
        return;
      }
      
      lookupTabletServerRetry(row.getKey());
      
      //MapFile에 Write
      writeToMapFile(row.getKey(), columnValues);
      
      previousKey = row.getKey();
    } catch (IOException e) {
      LOG.error(e);
      rollback();
      throw e;
    }
  }
  
  private void writeToMapFile(Row.Key rowKey, ColumnValue[][] columnValues) throws IOException {
    for(int i = 0; i < columnValues.length; i++) {
      for(ColumnValue eachColumnValue: columnValues[i]) {
        mapFileWriters.get(columnNames[i]).write(eachColumnValue);
      }
    }
  }
  
  public void rollback() throws IOException {
    if(currentTabletInfo == null) {
      return;
    }
    
    if(leaseToucher != null) {
      leaseToucher.interrupt();
    }
    
    LOG.info(currentTabletInfo.getTabletName() + " DirectUploader rollback, all temp files remove.");
    
    for(int i = 0; i < columnNames.length; i++) {
      MapFileWriter mapFileWriter = mapFileWriters.get(columnNames[i]);
      mapFileWriter.close();
      TabletMapFile mapFile = mapFiles.get(columnNames[i]);
        
      try {
        mapFile.delete();
      } catch (Exception e) {
        LOG.error(e);
      }
      
      try {
        LOG.info("Call cancelBatchUploader:" + currentTabletInfo);
        DataServiceProtocol tabletServer = CTableManager.connectTabletServer(currentTabletInfo, ctable.getConf());
        tabletServer.cancelBatchUploader(uploadActionId, currentTabletInfo.getTabletName());
      } catch (IOException e) {
        LOG.error("Can't cancel batch uploader:" + currentTabletInfo, e);
      }      
    }
  }
  
  /**
   * Send data to DFS, and add data file to TabletServer.<BR>
   * After putting all datas, you must call close() method
   * @throws IOException
   */
  public synchronized void close() throws IOException {
    LOG.info("Started Uploader closing:" + ctable.getTableName());
    internalClose();
    
    LOG.info("End internalClose:" + ctable.getTableName());
    LOG.info("Ended Uploader closing");
  }
  
  private void cancelUpload() throws IOException {
    //TODO 
  }
  
  private synchronized void internalClose() throws IOException {
    LOG.info("DirectUploader.internalClose: " + currentTabletInfo);
    if(currentTabletInfo == null) {
      return;
    }
    
    try {
      if(leaseToucher != null) {
        leaseToucher.interrupt();
      }      
      //send map file info to tabletserver
      if(mapFiles != null) {
        String[] mapFileIds = new String[columnNames.length];
        String[] mapFilePaths = new String[columnNames.length];
        
        for(int i = 0; i < columnNames.length; i++) {
          mapFileWriters.get(columnNames[i]).close();
          mapFileIds[i] = mapFiles.get(columnNames[i]).getFileId();
          mapFilePaths[i] = mapFiles.get(columnNames[i]).getFilePath().toString();
        }
        int retry = 0;
        IOException err = null;
        Row.Key targetRowKey = currentTabletInfo.getEndRowKey();
        TabletInfo tabletTabletInfo = currentTabletInfo;
        String taskId = null;
        DataServiceProtocol tabletServer = null;
        while(true) {
          try {
            if(tabletTabletInfo == null || tabletTabletInfo.getAssignedHostName() != null) {
              tabletTabletInfo = ctable.lookupTargetTablet(targetRowKey);
            }
            if(tabletTabletInfo.getAssignedHostName() != null) {
              tabletServer = CTableManager.connectTabletServer(tabletTabletInfo.getAssignedHostName(), ctable.getConf());
              taskId = tabletServer.endBatchUploader(uploadActionId, 
                  tabletTabletInfo.getTabletName(), 
                  columnNames, 
                  mapFileIds, 
                  mapFilePaths);
              break;
            } else {
              
            }
          } catch (IOException e) {
            LOG.warn("Error Uploader.internalClose():" + tabletTabletInfo + "," + e.getMessage() + ", but retry", e);
            err = e;
            ctable.locationCache.clearTabletCache(ctable.getTableName(), targetRowKey, tabletTabletInfo);
          }
          Thread.sleep(2 * 1000);
          retry++;
          if(retry > 30) {
            LOG.error("Error Uploader.internalClose():" + tabletTabletInfo + "," + err.getMessage(), err);
            throw err;
          }
        }
        
        long startTime = System.currentTimeMillis();
        while(true) {
          //FIXME Event 처리로 변경
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
              LOG.warn("No Upload closing response from tablerserer(" + tabletTabletInfo.getAssignedHostName() + "). " + 
                  "tablet=" + tabletTabletInfo.getTabletName());
              //다른 TabletServer로 한번 더 시도한다.
              try {
                Thread.sleep(5 * 1000);
                if(tabletTabletInfo != null) {
                  ctable.locationCache.clearTabletCache(tabletTabletInfo.getTableName(), 
                      targetRowKey, tabletTabletInfo);
                }
                TabletInfo lookupedTabletInfo = ctable.lookupTargetTablet(targetRowKey);
                if(lookupedTabletInfo != null) {
                  //Tablet의 host가 변경된 경우
                  if(!lookupedTabletInfo.getAssignedHostName().equals(tabletTabletInfo.getAssignedHostName())) {
                    tabletServer = CTableManager.connectTabletServer(lookupedTabletInfo, ctable.getConf());
                    taskId = tabletServer.endBatchUploader(
                        uploadActionId,
                        tabletTabletInfo.getTabletName(), 
                        columnNames, mapFileIds, mapFilePaths);
                    tabletTabletInfo = lookupedTabletInfo;
                  }
                }
              } catch (Exception e) {
                LOG.error("Error while retrying endBatchUploader(" + tabletTabletInfo.getAssignedHostName() + "). " + 
                    "tablet=" + tabletTabletInfo.getTabletName(), e);
              }            
            }   
          } catch (IOException e) {
            LOG.error("Error while retrying closeBatchUploader(" + tabletTabletInfo.getAssignedHostName() + "). " + 
                "tablet=" + tabletTabletInfo.getTabletName(), err);
          }
          Thread.sleep(2 * 1000);
          if(System.currentTimeMillis() - startTime > 180 * 1000) {
            //FIXME 이 경우 서버에서는 계속 처리하고 있을 가능성이 있다.
            //어떻게 해야 하나?
            LOG.error("Timeout while closeBatchUploader(" + (System.currentTimeMillis() - startTime) + " ms)" + tabletTabletInfo);
            throw new IOException("Timeout while closeBatchUploader(" + (System.currentTimeMillis() - startTime) + " ms)" + tabletTabletInfo);
          }
        }
      } 
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error(e);
      try {
        rollback();
      } catch (Exception err) {
        LOG.error(err);
      }
      throw new IOException(e.getMessage());
    } finally {
      currentTabletInfo = null;
    }
  }
}
