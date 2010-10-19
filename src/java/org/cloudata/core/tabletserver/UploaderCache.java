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
package org.cloudata.core.tabletserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.IdGenerator;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;


public class UploaderCache {
  public static final Log LOG = LogFactory.getLog(UploaderCache.class
      .getName());

  private TabletInfo tabletInfo;

  private Map<String, ColumnCollection> columnCollections;

  private long size;

  private String[] columnNames;

  private int addedCount = 0;

  private CloudataConf conf;

  private Merger merger = new Merger();
  
  private final long uploadBufferSize;

  private boolean mergeEnd = false; 
  
  private long maxMemory;
  
  private long forceSavingMemory;
  
  private String uploadActionId;
  
  private int numOfVersion;
  
  //the overhead of the arrays in memory 
  //12 => 4 for keyoffsets, 4 for keylengths, 4 for valueLengths, and
  //4 for indices into startOffsets array in the
  //pointers array (ignored the partpointers list itself)
  static private final int BUFFERED_KEY_VAL_OVERHEAD = 16;
  
  public UploaderCache(CloudataConf conf, TabletInfo tabletInfo,
      String[] columnNames, String uploadActionId, int numOfVersion) throws IOException {
    this.tabletInfo = tabletInfo;
    this.numOfVersion = numOfVersion;
    this.columnNames = columnNames;
    this.conf = conf;
    this.uploadBufferSize = conf.getInt("uploader.memory.buffer", 50) * 1024 * 1024; 
//    this.uploadCompactionDataSize = conf.getInt("uploader.compaction.data", 50) * 1024 * 1024;
//    this.maxMemory = SizeOf.maxMemory(); 
//    this.forceSavingMemory = (long)((float)maxMemory * 0.3f);
    this.uploadActionId = uploadActionId;
    //LOG.info("forceSavingMemory:" + forceSavingMemory);
    this.merger.start();
    init();
  }

  public void addColumnRecords(Row.Key rowKey, ColumnValue[][] columnValues)
      throws IOException {
    for (int i = 0; i < columnValues.length; i++) {
      String columnName = columnNames[i];
      ColumnCollection columnCollection = columnCollections.get(columnName);
      // if null, first insert in column
      if (columnCollection == null) {
        columnCollection = new ColumnCollection(conf);
        columnCollections.put(columnName, columnCollection);
      }
      for (int j = 0; j < columnValues[i].length; j++) {
        if (columnValues[i][j] != null) {
          // LOG.info("addColumnRecords:" + columnValues.length + "," +
          // columnValues[i].length + "," + columnValues[i][j].toString());
          size += columnCollection.addValue(rowKey, columnValues[i][j], numOfVersion);
          
          // size += columnValues[i][j].size();
        }
      }
    }
    addedCount++;
    
//    boolean neededSave = false;
//    if(addedCount % 500 == 0) {
//      long freeMemory = SizeOf.freeMemory();
//      long totalMemory = SizeOf.totalMemory();
//      
//      if((maxMemory - (totalMemory - freeMemory)) < forceSavingMemory) {
//        LOG.info("maxMemory=" + maxMemory + ", used=" + (totalMemory - freeMemory));
//        neededSave = true;
//      }
//    }
//    
//    //FIXME 실제 데이터의 크기와 메모리 사용량에 대한 비율(현재는 2)을 찾아서 재 설정
//    if(neededSave || size * BUFFERED_KEY_VAL_OVERHEAD > uploadCompactionDataSize) {
//      saveToDisk();
//    }
    
    if(size > uploadBufferSize) {
      saveToDisk();
    }
  }

  public void saveToDisk() throws IOException {
    if(merger.exception != null) {
      throw merger.exception;
    }
    Map<String, TabletMapFile> savedMapFiles = saveMemoryToDisk();
    
    if(savedMapFiles != null) {
      merger.addTabletMapFiles(savedMapFiles);
    }
    init();
  }

  private Map<String, TabletMapFile> saveMemoryToDisk() throws IOException {
    if (columnCollections == null || columnCollections.isEmpty()) {
      return null;
    }
    Map<String, TabletMapFile> savedMapFiles = new HashMap<String, TabletMapFile>();
    for (Map.Entry<String, ColumnCollection> entry : columnCollections.entrySet()) {
      String columnName = entry.getKey();
      TabletMapFile mapFile = entry.getValue().saveToLocalDisk(
          CloudataFileSystem.get(conf, "local"),
          tabletInfo,
          columnName, IdGenerator.getId(),
          uploadActionId,
          numOfVersion);
      
      if(mapFile != null) {
        savedMapFiles.put(columnName, mapFile);
      } else {
        LOG.info("saveMemoryToDisk: mapFile is null:columnName=" + columnName);
      }
      //LOG.info("saveMemoryToDisk: " + mapFile.getFilePath() + ",size=" + mapFile.getDataFileSize());
    }
    return savedMapFiles;
  }
  
  public Map<String, TabletMapFile> close() throws IOException {
    //남아 있는 데이터를 저장한다.
    LOG.info("Start close:" + tabletInfo);
    try {
      Map<String, TabletMapFile> savedMapFiles = saveMemoryToDisk();
      
      if(savedMapFiles != null) {
        merger.addTabletMapFiles(savedMapFiles);
        init();
      }
      LOG.info("merger.addTabletMapFiles()");
      
      merger.requestStop();
      
      while(!merger.ended) {
        try {
          Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
          return null;
        }
        if(merger.exception != null) {
          throw merger.exception;
        }
      }
      
      LOG.info("merger.ended");
      //최종적으로 merge 하면서 전송한다.
      Map<String, TabletMapFile> result = merger.mergeAndSend();
      
      //로컬 파일 삭제
      GPath path = Tablet.getTabletLocalTempPath(conf, tabletInfo, uploadActionId);
      CloudataFileSystem localFs = CloudataFileSystem.get(conf, "local");
      localFs.delete(path, true);
      
      LOG.info("End close:" + tabletInfo);
      return result;
    } catch (Exception e) {
      merger.requestStop();
      if(e instanceof IOException) {
        throw (IOException)e;
      } else {
        IOException err = new IOException(e.getMessage());
        err.initCause(e);
        throw err;
      }
    }
  }
  
  public void init() {
    if (columnCollections != null) {
      columnCollections.clear();
      columnCollections = null;
    }
    size = 0;
    columnCollections = Collections
        .synchronizedMap(new HashMap<String, ColumnCollection>(10));
    Runtime.getRuntime().gc();
  }

  class Merger extends Thread {
    private static final int NUM_MERGE_TARGET = 15;
    
    private IOException exception;
    
    private boolean stop = false;
    
    private boolean ended = false;
    
    //ColumnName -> TabletMapFiles
    private Map<String, List<TabletMapFile>> tabletMapFiles = new HashMap<String, List<TabletMapFile>>();

    private Map<String, List<TabletMapFile>> mergedFiles = 
      new HashMap<String, List<TabletMapFile>>();
    
    /**
     * 저장되어 있는 map file에서 merge 대상 파일을 빼낸다.
     * @return
     */
    private synchronized Map<String, List<TabletMapFile>> initMergeTarget() {
      Map<String, List<TabletMapFile>> mergeTargetMapFiles = 
          new HashMap<String, List<TabletMapFile>>();
      
      //merge 하기 위한 target 목록에 추가
      for(Map.Entry<String, List<TabletMapFile>> entry : tabletMapFiles.entrySet()) {
        String columnName = entry.getKey();
        List<TabletMapFile> mapFiles = entry.getValue();
        
        int mapFileSize = mapFiles.size();
        int numOfAdd = mapFiles.size() > NUM_MERGE_TARGET ? NUM_MERGE_TARGET : mapFiles.size();
        
        List<TabletMapFile> targetColumnFiles = new ArrayList<TabletMapFile>();
        for(int i = 0; i < numOfAdd; i++) {
          TabletMapFile tabletMapFile = mapFiles.remove(0);
          if(tabletMapFile != null) {
            targetColumnFiles.add(tabletMapFile);
          } else {
            //FIXME 언제 이런 경우가 발생하나?
            LOG.debug("mapFileSize:" + mapFileSize + ",numOfAdd:" + numOfAdd + ",currentMapFiles:" + mapFiles.size());
          }
        }
        mergeTargetMapFiles.put(columnName, targetColumnFiles);
      }
      
      return mergeTargetMapFiles;
    }
    
    public void run() {
      synchronized(tabletMapFiles) {
        while(!stop) {
          try {
            tabletMapFiles.wait();
          } catch (InterruptedException e) {
          }
          //LOG.info("meger.stop=" + stop);
          if(stop) {
            break;
          }
          
          int maxMapFileCount = getMaxMapFileCount(); 
          if(maxMapFileCount < NUM_MERGE_TARGET) {
            continue;
          }
          
          Map<String, List<TabletMapFile>> mergeTargetMapFiles = initMergeTarget();
          
          try {
            LOG.info("merge1: Start:" + tabletInfo.getTabletName() + ",maxMapFileCount=" + maxMapFileCount);
            merge(mergeTargetMapFiles, true);
          } catch (IOException e) {
            exception = e;
            LOG.error(e);
            return;
          }
        }
      }

      LOG.info("outer of while");
      
      //나머지 파일을 merge 한다.
      while(getMaxMapFileCount() > 0) {
        Map<String, List<TabletMapFile>> mergeTargetMapFiles = initMergeTarget();
        try {
          LOG.info("merge2: Start:" + tabletInfo.getTabletName());
          merge(mergeTargetMapFiles, true);
        } catch (IOException e) {
          exception = e;
          LOG.error(e);
          break;
        }
      }
      
      ended = true;
    }

    private Map<String, TabletMapFile> merge(Map<String, List<TabletMapFile>> mergeTargetMapFiles, 
        boolean local) throws IOException {
      if(exception != null) {
        throw exception;
      }
      String fileId = IdGenerator.getId() ;
      
      Map<String, TabletMapFile> resultMapFiles = new HashMap<String, TabletMapFile>(5);
      
      CloudataFileSystem resultFs = null;
      GPath resultParentPath = null;
      try {
        if(local) {
          resultFs = CloudataFileSystem.get(conf, "local");
          resultParentPath = Tablet.getTabletLocalTempPath(conf, tabletInfo, uploadActionId);
        } else {
          resultFs = CloudataFileSystem.get(conf);
          resultParentPath = Tablet.getTabletUploadTempPath(conf, tabletInfo, uploadActionId);
        }
        
        for(Map.Entry<String, List<TabletMapFile>> entry : mergeTargetMapFiles.entrySet()) {
          String columnName = entry.getKey();
          List<TabletMapFile> entryMapFiles = entry.getValue();
          GPath resultPath = new GPath(resultParentPath, columnName + "/" + fileId + "/");

          LOG.info("Merger.merge: Start:columnName=" + columnName + 
              ",entryMapFiles=" + entryMapFiles.size() + ",local=" + local + ",path=" + resultPath);
          
          TabletMapFile resultFile = 
            DiskSSTable.mergeColumnMapFiles(
                conf, 
                resultFs,
                resultPath,
                tabletInfo,
                fileId, 
                columnName, 
                entryMapFiles,
                numOfVersion);
          
          resultMapFiles.put(columnName, resultFile);
        }
        
        //merge 하기 이전 파일 삭제
        for(Map.Entry<String, List<TabletMapFile>> entry : mergeTargetMapFiles.entrySet()) {
          List<TabletMapFile> entryMapFiles = entry.getValue();
          deleteOriginFile(entryMapFiles);
        }
      } catch (Exception e) {
        //예외가 발생하면 major compaciton temp에 있는 모든 내용 삭제
        LOG.error("merge error:merge:" + tabletInfo.getTabletName(), e);
        resultFs.delete(resultParentPath, true);
        throw new IOException(e.getMessage());
      }
      LOG.info("merge: End:" + tabletInfo.getTabletName());
      
      //map file 목록에 merge 결과 파일 추가
      addMergedFiles(resultMapFiles);
      
      LOG.info("Merger.merge: End");
      return resultMapFiles;
    }

    private Map<String, TabletMapFile> mergeAndSend() throws IOException {
      return merge(mergedFiles, false);
    }
    
    /** 
     * 로컬에 저장된 파일을 추가한다.
     * @param savedMapFiles
     */
    private void addTabletMapFiles(Map<String, TabletMapFile> savedMapFiles) {
      synchronized(tabletMapFiles) {
        for(Map.Entry<String, TabletMapFile> entry: savedMapFiles.entrySet()) {
          String columnName = entry.getKey();
          TabletMapFile savedMapFile = entry.getValue();
          
          List<TabletMapFile> columnMapFiles = tabletMapFiles.get(columnName);
          if (columnMapFiles == null) {
            columnMapFiles = new ArrayList<TabletMapFile>(10);
            tabletMapFiles.put(columnName, columnMapFiles);
          }
          columnMapFiles.add(savedMapFile);
        }
        tabletMapFiles.notifyAll();
      }
    }
    
    /**
     * Merge 결과를 추가한다.
     * @param resultMapFiles
     */
    private void addMergedFiles(Map<String, TabletMapFile> resultMapFiles) {
      synchronized(mergedFiles) {
        for(Map.Entry<String, TabletMapFile> entry : resultMapFiles.entrySet()) {
          String columnName = entry.getKey();
          TabletMapFile mapFile = entry.getValue();
          
          List<TabletMapFile> mapFiles;
          if(mergedFiles.containsKey(columnName)) {
            mapFiles = mergedFiles.get(columnName);
          } else {
            mapFiles = new ArrayList<TabletMapFile>();
            mergedFiles.put(columnName, mapFiles);
          }
          mapFiles.add(mapFile);
        }
      }
    }
    
    private void deleteOriginFile(List<TabletMapFile> entryMapFiles) throws IOException {
      for(TabletMapFile eachMapFile: entryMapFiles) {
        eachMapFile.delete();
      }
    }
    
    private int getMaxMapFileCount() {
      int max = 0;
      for(Map.Entry<String, List<TabletMapFile>> entry : tabletMapFiles.entrySet()) {
        List<TabletMapFile> mapFiles = entry.getValue();
        if(mapFiles.size() > max) {
          max = mapFiles.size();
        }
      }
      
      return max;
    }
    
    public void requestStop() {
      //LOG.info("start requestStop()");
      synchronized(tabletMapFiles) { 
        //LOG.info("start requestStop1()");
        stop = true;
        tabletMapFiles.notifyAll();
        //LOG.info("start requestStop2()");
      }
    }
  }
  

  public static void main(String[] args) throws IOException {
    String[] fileIds = {
      "2103350082532633",
      "2103410633800127",
      "2103450741066199",
      "2103500346333144",
      "2103559783033356",
      "2103586661083402",
      "2103626817472867",
      "2103666981694322",
      "2103706604873648",
      "2103742403745199",
      "2103789708201200",
      "2103824404629707",
      "2103864347268650",
      "2103911377307657",
      "2103945985871161",
      "2103977207119338",
      "2104017666645030",
      "2104062617994125",
      "2104098646512260",
      "2104151957046260",
      "2104189871621808",
      "2104220387055598",
      "2104250544171655",
      "2104306679691066",
      "2104347031094634",
      "2104375542451161",
      "2104409996044775",
      "2104440045199409",
      "2104528819584839",
      "2104584534174613",
      "2104622740802583",
      "2104663172690124",
      "2104699272007211",
      "2104725309418466",
      "2104760457259391",
      "2104785837085906",
      "2104814185087375",
      "2104843366419546",
      "2104874723930689",
      "2104905720625847",
      "2104935431663363",
      "2104960037226740",
      "2104986121318344",
      "2105044359197383",
      "2105089746482980",
      "2105124562608814",
      "2105158133974684",
      "2105210701379730",
      "2105249380523802",
      "2105296374674414",
      "2105329972152814"};
    
    
    List<TabletMapFile> mapFiles = new ArrayList<TabletMapFile>();
    
    CloudataConf conf = new CloudataConf();
    
    CloudataFileSystem fs = CloudataFileSystem.get(conf, "local");
    
    TabletInfo tabletInfo = new TabletInfo();
    tabletInfo.setTableName("T_MUSIC");
    tabletInfo.setTabletName("T_MUSIC_10");
    
    for(String eachId: fileIds) {
      TabletMapFile mapFile = new TabletMapFile(conf, fs,
          tabletInfo, "item", eachId, new GPath("/home1/cloudata/temp/" + eachId),3);
      
      mapFile.loadIndex();
      mapFiles.add(mapFile);
    }
        
    try {
      String columnName = "item";

      GPath resultPath = new GPath("/user/cloudata/result/01");
      
      CloudataFileSystem resultFs = CloudataFileSystem.get(conf);
      TabletMapFile resultFile = 
        DiskSSTable.mergeColumnMapFiles(
            conf, 
            resultFs,
            resultPath,
            tabletInfo,
            "01", 
            columnName, 
            mapFiles,
            3);
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOG.info("merge: End:" + tabletInfo.getTabletName());
  }
}
