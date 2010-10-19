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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.IdGenerator;
import org.cloudata.core.common.CStopWatch;
import org.cloudata.core.common.CloudataLock;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.TestCaseTabletServerKillException;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileReader;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileWriter;


public class DiskSSTable  {
  public static final Log LOG = LogFactory.getLog(DiskSSTable.class.getName());
  
  protected TabletInfo tabletInfo;

  //columnName, MapFiles
  protected Map<String, List<TabletMapFile>> mapFiles = Collections.synchronizedMap(new HashMap<String, List<TabletMapFile>>());
  
  //columnName, CacheTable
  protected Map<String, ColumnMemoryCache> columnMemoryCaches = Collections.synchronizedMap(new HashMap<String, ColumnMemoryCache>()); 

  protected Set<String> notUseColumnMemoryCache = Collections.synchronizedSet(new HashSet<String>());  
  
  int maxMapFileCount = 0;
  
  long totalMapFileSize = 0;

  protected final CloudataLock lock = new CloudataLock();
  
  protected CloudataFileSystem fs;
  
  protected CloudataConf conf;
  
  protected TabletServer tabletServer;
  
  private int numOfVersion;
  
  private Tablet tablet;
  
  public void init(TabletServer tabletServer, CloudataConf conf, Tablet tablet,
      int numOfVersion) {
    this.tablet = tablet;
    this.tabletInfo = tablet.getTabletInfo();
    this.conf = conf;
    this.fs = CloudataFileSystem.get(conf);
    this.tabletServer = tabletServer;
    this.numOfVersion = numOfVersion;
  }

  public void clearAllMemory() {
    lock.obtainWriteLock();
    try {
      mapFiles.clear();
      columnMemoryCaches.clear();
      maxMapFileCount = 0;
      totalMapFileSize = 0;
    } finally {
      lock.releaseWriteLock();
    }
  }
  
  public TabletInfo getTabletInfo() {
    return tabletInfo;
  }
  
//  public Map<String, List<TabletMapFile>> getMapFiles() {
//    return mapFiles;
//  }
  
  public Scanner[] getMapFileScanners(String columnName, Row.Key startRowKey, 
      Row.Key endRowKey, 
      CellFilter cellFilter) throws IOException {
    lock.obtainReadLock();
    try {
      List<TabletMapFile> tabletMapFiles =  mapFiles.get(columnName);
      if(tabletMapFiles == null) {
        return null;
      }
      Scanner[] scanners = new Scanner[tabletMapFiles.size()];
      int index = 0;
      for(TabletMapFile eachMapFile: tabletMapFiles) {
        scanners[index] = eachMapFile.getMapFileReader(startRowKey, endRowKey, cellFilter);
        index++;
      }
      return scanners;
    } finally {
      lock.releaseReadLock();
    }
  }
  
  public void truncateColumn(String columnName) throws IOException {
    lock.obtainWriteLock();
    try {
      List<TabletMapFile> mapFileList = mapFiles.get(columnName);
      if(mapFileList != null) {
        for(TabletMapFile eachMapFile: mapFileList) {
          eachMapFile.delete();
        }
      }
      mapFiles.remove(columnName);
      calculateMaxMapFileCount();
      calculateTotalMapFileSize();
    } finally {
      lock.releaseWriteLock();
    }
  }
  
  public SortedSet<MapFileIndexRecord> getMapFileIndex() {
    lock.obtainReadLock();
    try {
      SortedSet<MapFileIndexRecord> result = new TreeSet<MapFileIndexRecord>();
      for(List<TabletMapFile> eachMapFiles: mapFiles.values()) {
        for(TabletMapFile tabletMapFile: eachMapFiles) {
          SortedSet<MapFileIndexRecord> indexRecords = tabletMapFile.getMapFileIndexRecords();
          result.addAll(indexRecords);
        }
      }
      
      return result;
    } finally {
      lock.releaseReadLock();
    }
  }
  
  public void addTabletMapFile(String columnName, TabletMapFile mapFile) throws IOException {
    lock.obtainWriteLock();
    try {
      List<TabletMapFile> mapFileList = mapFiles.get(columnName);
      if(mapFileList == null) {
        mapFileList = Collections.synchronizedList(new ArrayList<TabletMapFile>());
        mapFiles.put(columnName, mapFileList);
      }
      
      mapFileList.add(mapFile);
      if (mapFileList.size() > this.maxMapFileCount) {
        this.maxMapFileCount = mapFileList.size(); 
      }
      calculateTotalMapFileSize();
    } finally {
      lock.releaseWriteLock();
    }
  }
  
  public void removeTabletMapFile(String columnName, TabletMapFile mapFile) throws IOException {
    lock.obtainWriteLock();
    try {
      List<TabletMapFile> mapFileList = mapFiles.get(columnName);
      if(mapFileList != null) {
        mapFileList.remove(mapFile);
      }
      calculateMaxMapFileCount();
      calculateTotalMapFileSize();
    } finally {
      lock.releaseWriteLock();
    }
  }
  
  public void load() throws IOException {
    try {
      if(fs.exists(Tablet.getTabletMajorCompactionTempPath(conf, tabletInfo))) {
        fs.delete(Tablet.getTabletMajorCompactionTempPath(conf, tabletInfo), true);
      }
      if(fs.exists(Tablet.getTabletMinorCompactionTempPath(conf, tabletInfo))) {
        fs.delete(Tablet.getTabletMinorCompactionTempPath(conf, tabletInfo), true);
      }
      if(fs.exists(Tablet.getTabletSplitTempPath(conf, tabletInfo))) {
        fs.delete(Tablet.getTabletSplitTempPath(conf, tabletInfo), true);
      }
    } catch (Exception e) {
      LOG.error("Can't delete temp file while loding:" + tabletInfo, e);
    }
    
    deleteUnusedFiles();
    
    GPath[] columnPaths = fs.list(Tablet.getTabletPath(conf, tabletInfo));
    
    if(columnPaths == null || columnPaths.length == 0) {
      return;
    }
    for(GPath eachColumnPath: columnPaths) {
      String pathName = eachColumnPath.getName();
      GPath[] mapFilePaths = fs.list(eachColumnPath);
      
      if(mapFilePaths == null || mapFilePaths.length == 0) {
        continue;
      }
      
      for(GPath eachMapFilePath: mapFilePaths) {
        TabletMapFile tabletMapFile = new TabletMapFile(conf, tabletInfo, pathName, eachMapFilePath.getName(), numOfVersion);
        if(tabletMapFile.loadIndex()) { 
          addTabletMapFile(pathName, tabletMapFile);
        }
      }
    }
    
    //loadToMemoryCache();
  }

  public ColumnValue search(Row.Key rowKey, String columnName, Cell.Key columnKey) throws IOException {
    lock.obtainReadLock();
    try {
      List<TabletMapFile> tabletMapFiles = mapFiles.get(columnName);
      if(tabletMapFiles == null || tabletMapFiles.isEmpty()) {
        return null;
      }
      
      ValueCollection valueCollection = new ValueCollection();
      for(TabletMapFile tabletMapFile: tabletMapFiles) {
        valueCollection.addAll(tabletMapFile.scan(rowKey, columnKey), numOfVersion);
      }   
      return valueCollection.get();
    } finally {
      lock.releaseReadLock();
    }
  }
  
  public ValueCollection searchAllVersion(Row.Key rowKey, String columnName, Cell.Key columnKey) throws IOException {
    lock.obtainReadLock();
    try {    
      List<TabletMapFile> tabletMapFiles = mapFiles.get(columnName);
      if(tabletMapFiles == null || tabletMapFiles.isEmpty()) {
        return null;
      }
      
      ValueCollection valueCollection = new ValueCollection();
      for(TabletMapFile tabletMapFile: tabletMapFiles) {
        valueCollection.addAll(tabletMapFile.scan(rowKey, columnKey), numOfVersion);
      }   
      if(valueCollection.isEmpty()) {
        return null;
      } else {
        return valueCollection;
      }
    } finally {
      lock.releaseReadLock();
    }
  }
  
  public Collection<ColumnValue> search(Row.Key rowKey, String columnName) throws IOException {
    //하나의 Row.Key에 해당하는 모든 Cell.Key에 대한 value를 가져온다.
    //각각의 Map 파일에 흩어져서 저장되고 있기 때문에 가가 Map파일에서 가져온 정보를 merge해야 한다.
    lock.obtainReadLock();
    try {    
      List<TabletMapFile> tabletMapFiles = mapFiles.get(columnName);

      if(tabletMapFiles == null || tabletMapFiles.isEmpty()) {
        return new ArrayList<ColumnValue>();      
      }
      
      if(tabletMapFiles.size() == 1) {
        return tabletMapFiles.get(0).scan(rowKey);
      }
      
      MapFileReader[] mapFileReaders = new MapFileReader[tabletMapFiles.size()];
      int index = 0;
      for(TabletMapFile tabletMapFile: tabletMapFiles) {
        MapFileReader reader = tabletMapFile.getMapFileReader(rowKey, rowKey);
        mapFileReaders[index++] = reader;
      }
      
      //merge
      return mergeColumnValueFromMapFile(mapFileReaders);
    } finally {
      lock.releaseReadLock();
    }
  }
  
  private List<ColumnValue> mergeColumnValueFromMapFile(MapFileReader[] mapFileReaders) throws IOException {
    List<ColumnValueEntry> workPlace = new ArrayList<ColumnValueEntry>(mapFileReaders.length);
    
    //초기화
    for(int i = 0; i < mapFileReaders.length; i++) {
      if(mapFileReaders[i] != null) {
        ColumnValue columnValue = mapFileReaders[i].next();
        if(columnValue != null) {
          workPlace.add(new ColumnValueEntry(columnValue, i));
        } else {
          mapFileReaders[i].close();
          mapFileReaders[i] = null;
        }
      }
    }
    
    List<ColumnValue> result = new ArrayList<ColumnValue>();
    
    boolean end = false;
    while(!end) {
      int size = workPlace.size();
      if(size == 0) {
        break;
      }
      
      Collections.sort(workPlace);
      
      ColumnValueEntry winner = workPlace.remove(0);
      result.add(winner.columnValue);
      
      ColumnValue columnValue = mapFileReaders[winner.index].next();
      
      if(columnValue != null) {
        workPlace.add(new ColumnValueEntry(columnValue, winner.index));
      } else {
        mapFileReaders[winner.index].close();
        mapFileReaders[winner.index] = null;
        
        end = true;
        for(int i = 0; i < mapFileReaders.length; i++) {
          if(mapFileReaders[i] != null) {
            columnValue = mapFileReaders[i].next();
            if(columnValue != null) {
              workPlace.add(new ColumnValueEntry(columnValue, i));
              end = false;
              break;
            } else {
              mapFileReaders[i].close();
              mapFileReaders[i] = null;
            }
          }
        }
      }
    } //end of while
    
    return result;
  }
  
  static class ColumnValueEntry implements Comparable<ColumnValueEntry> {
    ColumnValue columnValue;
    int index;
    
    public ColumnValueEntry(ColumnValue columnValue, int index) {
      this.columnValue = columnValue;
      this.index = index;
    }

    public int compareTo(ColumnValueEntry entry) {
      return columnValue.compareTo(entry.columnValue);
    }

    public boolean equals(ColumnValueEntry obj) {
      if( !(obj instanceof ColumnValueEntry) ) {
        return false;
      }
      
      return compareTo((ColumnValueEntry)obj) == 0;
    }  
  }    
  
  /**
   * minor Compaction 도중 오류가 발생했을 때 파일 내용을 모두 지우는 작업 수행 
   * @throws IOException
   */
  public void cancelCompaction(String fileId) throws IOException {
    lock.obtainWriteLock();
    try {
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        List<TabletMapFile> columnMapFiles = entry.getValue();
        List<TabletMapFile> tempColumnMapFiles = new ArrayList<TabletMapFile>();
        tempColumnMapFiles.addAll(columnMapFiles);
        for(TabletMapFile mapFile: tempColumnMapFiles) {
          if(fileId.equals(mapFile.getFileId())) {
            //1. 메모리에서 제거 
            columnMapFiles.remove(mapFile);
            //2.파일제거
            mapFile.delete();
          }
        }
      }
      
      calculateMaxMapFileCount();
      calculateTotalMapFileSize();
    } finally {
      lock.releaseWriteLock();
    }
  }
  
  /**
   * 
   * @throws IOException
   */
  public void majorCompaction() throws IOException {
    //deleteUnusedFiles();
    
    Map<String, List<TabletMapFile>> oldMapFiles = new HashMap<String, List<TabletMapFile>>();
    
    //merge target
    CStopWatch watch = new CStopWatch();
    lock.obtainReadLock();

    try {
      for(Map.Entry<String, List<TabletMapFile>> entry : mapFiles.entrySet()){
        String columnName = entry.getKey();
        List<TabletMapFile> eachMapFiles = entry.getValue();
        if(eachMapFiles.size() > 1) {
          List<TabletMapFile> cloneMapFiles = new ArrayList<TabletMapFile>(10);
          cloneMapFiles.addAll(eachMapFiles);

          oldMapFiles.put(columnName, cloneMapFiles);
        }
      }
    } finally{
      lock.releaseReadLock();
    }
    
    //새로 생성할 Map 파일에 대한 ID
    String fileId = IdGenerator.getId() ;

    //merge
    Map<String, TabletMapFile> mergedMapFiles = merge(oldMapFiles, fileId);
    
    //move new
    moveMergedFile(mergedMapFiles);
    
    //add new map file list and remove old file
    try {
      addMergedFiles(oldMapFiles, mergedMapFiles);
    } catch (Exception e) {
      //여기서 장애가 발생하면 TabletServer를 shutdown 시킨다(장애가 발생할 특별한 이유가 없음)
      LOG.fatal("TabletServer shutdowned cause :" + e.getMessage(), e);
      tabletServer.shutdown();
      return;
    }
    
    //delete old file
    //삭제 도중 kill되거나 오류가 발생한 경우에는???

    watch.start("delete old files in major compaction", 1000);
    for(Map.Entry<String, TabletMapFile> entry : mergedMapFiles.entrySet()) {
      String columnName = entry.getKey();
      for(TabletMapFile oldMapFile: oldMapFiles.get(columnName)) {
        try {
          boolean deleteResult = oldMapFile.delete();
          if(!deleteResult) {
            LOG.warn("Fail deleting Old MapFile while MajorCompaction: " + oldMapFile);
          }
        } catch (Exception e) {
          //delete 하지 못하더라도 다음 majorCompaction 수행시 중복된 데이터는 자동으로 merge된다.
          LOG.error("merge:delete old file:" + oldMapFile, e);
        }
      }
      watch.stopAndReportIfExceed(LOG);
    } 
    
    //delete temp dir
    fs.delete(Tablet.getTabletMajorCompactionTempPath(conf, tabletInfo), true);
  }

  public void addMergedFiles(Map<String, 
      List<TabletMapFile>> targetMapFiles, 
      Map<String, TabletMapFile> mergedMapFiles) throws IOException {
    CStopWatch watch = new CStopWatch();
    watch.start("addMergedFiles lock.obtainWriteLock", 50);
    lock.obtainWriteLock();
    watch.stopAndReportIfExceed(LOG);
    try {
      for(Map.Entry<String, TabletMapFile> entry : mergedMapFiles.entrySet()) {
        String columnName = entry.getKey();
        TabletMapFile mergedMapFile = entry.getValue();
        
        //새로 생성된 map 파일 추가
        List<TabletMapFile> runningMapFiles = mapFiles.get(columnName);
        if(runningMapFiles != null) {
          runningMapFiles.add(mergedMapFile);
        }

        //기존 map file 목록 제거
        for(TabletMapFile mergeTargetMapFile: targetMapFiles.get(columnName)) {
          runningMapFiles.remove(mergeTargetMapFile);
        }
      }
      
      calculateMaxMapFileCount();
      calculateTotalMapFileSize();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void moveMergedFile(Map<String, TabletMapFile> mergedMapFiles) throws IOException {
    try {
      for(Map.Entry<String, TabletMapFile> entry : mergedMapFiles.entrySet()) {
        String columnName = entry.getKey();
        TabletMapFile mergedMapFile = entry.getValue();
        //FIXME 몇개만 move되고 exception 발생이 아니라 kill되는 경우에는 문제가 발생함.
        moveMajorTempToTablet(columnName, mergedMapFile);
      }
    } catch(TestCaseTabletServerKillException err) {
      LOG.error("merge error:move:" + tabletInfo.getTabletName(), err);
      throw new IOException(err.getMessage());
    } catch (Exception e) {
      LOG.error("merge error:move:" + tabletInfo.getTabletName(), e);
      //오류 발생 시 이동된 모든 내용을 지운다.
      for(Map.Entry<String, TabletMapFile> entry : mergedMapFiles.entrySet()) {
        TabletMapFile mergedMapFile = entry.getValue();
        try {
          if(mergedMapFile.delete()) {
            LOG.error("merge error:can't delete temp file cause error:" + mergedMapFile.getFilePath());
          }
        } catch (Exception e1) {
          LOG.error("merge error:delete temp file cause error", e1);
        }
      }
      
      //temp의 내용도 지운다.
      fs.delete(Tablet.getTabletMajorCompactionTempPath(conf, tabletInfo), true);
      throw new IOException(e.getMessage());
    }
  }

  public Map<String, TabletMapFile> merge(Map<String, List<TabletMapFile>> targetMapFiles, String fileId) throws IOException {
    Map<String, TabletMapFile> mergedMapFiles = new HashMap<String, TabletMapFile>(5);
    try {
      for(Map.Entry<String, List<TabletMapFile>> entry : targetMapFiles.entrySet()) {
        String columnName = entry.getKey();
        List<TabletMapFile> entryMapFiles = entry.getValue();
        long totalDataFileSize = 0;

        for(TabletMapFile mapFile : entryMapFiles) {
          totalDataFileSize += mapFile.getDataFileSize();
        }

        long startTime = System.currentTimeMillis();
        TabletMapFile resultFile = mergeMapFiles(fileId, columnName, entryMapFiles);
        //LOG.debug("merge:" + tabletInfo.getTabletName() + ", time=" + (System.currentTimeMillis() - startTime) + ", mapfile.count=" + entryMapFiles.size() + ", mapfile.size=" + totalDataFileSize);
        mergedMapFiles.put(columnName, resultFile);
        //targetMapFiles.put(columnName, mapFiles);
      }
    } catch (Exception e) {
      //예외가 발생하면 major compaciton temp에 있는 모든 내용 삭제
      LOG.error("merge error:merge:" + tabletInfo.getTabletName(), e);
      fs.delete(Tablet.getTabletMajorCompactionTempPath(conf, tabletInfo), true);
      throw new IOException(e.getMessage());
    }
    return mergedMapFiles;
  }

  public TabletMapFile mergeMapFiles(String fileId, String columnName, List<TabletMapFile> mergeMapFiles) throws IOException {
    GPath resultPath = new GPath(Tablet.getTabletMajorCompactionTempPath(conf, tabletInfo), columnName + "/" + fileId + "/");
    return mergeColumnMapFiles(conf, 
        CloudataFileSystem.get(conf),
        resultPath,
        tabletInfo,
        fileId,
        columnName,
        mergeMapFiles,
        numOfVersion
        );
  }
  
  public static TabletMapFile mergeColumnMapFiles(
      CloudataConf conf,
      CloudataFileSystem resultFs,
      GPath resultPath,
      TabletInfo tabletInfo,
      String fileId, 
      String columnName, 
      List<TabletMapFile> mergeMapFiles,
      int numOfVersion) throws IOException {
    FileMerger merger = new FileMerger();
    
    TabletMapFile resultFile = new TabletMapFile(conf, resultFs,
        tabletInfo, columnName, fileId, resultPath,
        numOfVersion
        );
    
    merger.merge(resultFile, mergeMapFiles.toArray(new TabletMapFile[mergeMapFiles.size()]), false, numOfVersion);
    return resultFile;
  }
  
  /**
   * 비정상적인 tabletserver의 종료에 의해 mapfile 작업 중 정상 처리되지 않는 파일에 대한 삭제 작업
   * @throws IOException
   */
  private void deleteUnusedFiles() throws IOException {
    //FIXME MinorCompation이 실행되고 있는 도중에 deleteUb=nusedFiles이 호출된 경우에는?
    
    //MajonCompaction 시 compaciton 작업 후 이전에 있던 파일 중 일부만 삭제되고 일부는 남아 있는 경우
    //MajorCompaction 수행 전에 MapFile 목록에 없는 파일은 삭제한다.     
    //LOG.debug("execute deleteUnusedFiles");
    GPath tabletPath = Tablet.getTabletPath(conf, tabletInfo);
    GPath[] columnPaths = fs.list(tabletPath);
    
    if(columnPaths == null) {
      return;
    }
    
    for(int i = 0; i < columnPaths.length; i++) {
      try {
        if(!fs.isDirectory(columnPaths[i])) {
          continue;
        }
        String columnName = columnPaths[i].getName();
        Set<String> validColumnMapFilesSet = new HashSet<String>();
        
        lock.obtainReadLock();
        try {
          List<TabletMapFile> columnMapFiles = mapFiles.get(columnName);
          
          if(columnMapFiles != null) {
            for(TabletMapFile eachMapFile: columnMapFiles) {
              validColumnMapFilesSet.add(eachMapFile.getFilePath().getName());
            }
          }
        } finally {
          lock.releaseReadLock();
        }
        
        //실제 파일 목록을 가져와서 비교한다.
        GPath[] mapFilePaths = fs.list(columnPaths[i]);
        if(mapFilePaths == null) {
          continue;
        }
        GPath tempPath = new GPath(Tablet.getTabletPath(conf, tabletInfo) + "/temp");
        for(int j = 0; j < mapFilePaths.length; j++) {
          if(validColumnMapFilesSet.size() > 0) {
            //Minor, Major Compacition 처리 이전에 작업하는 경우
            if(!validColumnMapFilesSet.contains(mapFilePaths[j].getName()) &&
                !mapFilePaths[j].toString().startsWith(tempPath.toString())) {
              LOG.info("deleteUnusedFiles1: delete file: " + mapFilePaths[j]);
              fs.delete(mapFilePaths[j], true);
            }
          } else {
            //Tablet 로딩 시 작업하는 경우(map file, idx file 모두 존재하지 않는 경우 삭제)
            if(mapFilePaths[j].toString().startsWith(tempPath.toString())) {
              continue;
            }
            if(!fs.exists(new GPath(mapFilePaths[j] + "/" + TabletMapFile.DATA_FILE)) ||
                !fs.exists(new GPath(mapFilePaths[j] + "/" + TabletMapFile.DATA_FILE))) {
              LOG.info("deleteUnusedFiles2: delete file: " + mapFilePaths[j]);
              fs.delete(mapFilePaths[j], true);
            }
          }
        }
      } catch (Exception e) {
        LOG.error("deleteUnusedFiles error: " + columnPaths[i] + ": " + e.getMessage());
      }
    }
    
    for(int i = 0; i < columnPaths.length; i++) {
      GPath[] childPaths = fs.list(columnPaths[i]);
      if(childPaths == null || childPaths.length == 0) {
        fs.delete(columnPaths[i]);
      }
    }
    
    calculateMaxMapFileCount();
    calculateTotalMapFileSize();
  }
  
  public void moveMajorTempToTablet(String columnName, TabletMapFile tabletMapFile) throws IOException {
    GPath targetPath = new GPath(Tablet.getTabletPath(conf, tabletInfo), columnName + "/" + tabletMapFile.getFileId() + "/");
    tabletMapFile.moveTo(targetPath, true);
  }
  
  private void calculateMaxMapFileCount() {
    maxMapFileCount = 0;
    try {
      for(List<TabletMapFile> mapFile : mapFiles.values()){
        if(mapFile.size() > maxMapFileCount) {
          maxMapFileCount = mapFile.size();
        }
      }
    } catch (Exception e) {
      LOG.warn("Error in calculating max map file count", e);
    }    
  }
  
  public int getMaxMapFileCount() {
    return maxMapFileCount;
  }

  public ColumnValue findClosestMeta(Row.Key rowKey, String columnName, boolean great) throws IOException {
    lock.obtainReadLock();
    try {
      if(columnMemoryCaches.containsKey(columnName)) {
        ColumnMemoryCache cache = columnMemoryCaches.get(columnName);
        return cache.findClosest(rowKey);
      }
      List<TabletMapFile> tabletMapFiles = mapFiles.get(columnName);
      if(tabletMapFiles == null || tabletMapFiles.isEmpty()) {
        return null;      
      }

      MapFileReader[] readers = new MapFileReader[tabletMapFiles.size()];

      TreeSet<MetaValue> metaValues = new TreeSet<MetaValue>();
      
      TreeSet<ColumnValue> workPlace = new TreeSet<ColumnValue>();
      
      try {
        //init
        CellFilter cellFilter = new CellFilter(columnName);
        int index = 0; 
        for(TabletMapFile tabletMapFile: tabletMapFiles) {
          MapFileReader reader = tabletMapFile.getMapFileReader(rowKey, Row.Key.MAX_KEY, cellFilter);
          ColumnValue columnValue = null;
          while( (columnValue = reader.next()) != null ) {
            if(great) {
              if(columnValue.getRowKey().compareTo(rowKey) < 0) {
                continue;
              }
            } else {
              if(columnValue.getRowKey().compareTo(rowKey) <= 0) {
                continue;
              }
            }
            break;
          }
          if(columnValue != null) {
            workPlace.add(columnValue);
            readers[index] = reader;
          } else {
            reader.close();
            readers[index] = null;
          }
          index++;
        }
  
        //findClosestMeta
        while(true) {
          if(workPlace.isEmpty()) {
            return null;
          } 
          ColumnValue winnerColumnValue = workPlace.first();
          metaValues.add(new MetaValue(winnerColumnValue));
          workPlace.remove(winnerColumnValue);
          Row.Key winnerRowKey = winnerColumnValue.getRowKey();
          
          List<ColumnValue> tempWorkPlace = new ArrayList<ColumnValue>();
          tempWorkPlace.addAll(workPlace);
          for(ColumnValue eachColumnValue: tempWorkPlace) {
            if(winnerRowKey.equals(eachColumnValue.getRowKey())) {
              metaValues.add(new MetaValue(eachColumnValue));
              workPlace.remove(eachColumnValue);
            }
          }
          
          for(int i = 0; i < readers.length; i++) {
            if(readers[i] == null) {
              continue;
            }
            ColumnValue columnValue = null;
            while( (columnValue = readers[i].next()) != null ) {
              if(winnerRowKey.equals(columnValue.getRowKey())) {
                
                metaValues.add(new MetaValue(columnValue));
              } else {
                workPlace.add(columnValue);
                break;
              }
            }
            if(columnValue == null) {
              readers[i].close();
              readers[i] = null;
            }
          }
          
          if(metaValues.size() > 0) {
            MetaValue firstValue = metaValues.first();
            if(!firstValue.columnValue.isDeleted()) {
              return firstValue.columnValue;
            } else {
              metaValues.clear();
            }
          }
        }
      } finally {
        for(int i = 0; i < readers.length; i++) {
          try {
            if(readers[i] != null) {
              readers[i].close();
            }
          } catch (Exception e) {
            LOG.warn("Can't close MapFileReader:" + e.getMessage());
          }
        }
      }
    } finally {
      lock.releaseReadLock();
    }
  }

  public boolean hasValue(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    lock.obtainReadLock();
    try {
      List<TabletMapFile> tabletMapFiles = mapFiles.get(columnName);
      if(tabletMapFiles == null || tabletMapFiles.isEmpty()) {
        return false;      
      }
      
      for(TabletMapFile tabletMapFile: tabletMapFiles) {
        if(tabletMapFile.hasValue(rowKey, cellKey)) {
          return true;
        }
      }
      return false;
    } finally {
      lock.releaseReadLock();
    }
  }
  
  /**
   * 
   * @throws IOException
   */
  public void printMapFileInfo() throws IOException {
    lock.obtainReadLock();
    try {
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        for(TabletMapFile mapFile: entry.getValue()) {
          LOG.info("printMapFileInfo:tabletName=" + tabletInfo.getTabletName() + ", path=" + mapFile.getFilePath() + ", size=" + mapFile.getDataFileSize());
        }
      }
    } finally {
      lock.releaseReadLock();
    }
  }
  
  /**
   * Tablet의 크기가 정해진 크기보다 큰 경우 Tablet을 두개로 쪼갠다.
   * Split은 Major Compaction을 통해 TabletMapFile을 하나라 만든 다음 처리한다.
   * split 결과는 두개의 DiskSSTable이 반환된다. 
   * @param createdTablets
   * @return
   */
  public void split(Row.Key midRowKey, Tablet[] createdTablets)  throws IOException {
    Map<String, List<TabletMapFile>> cloneMapFiles = null;
    
    lock.obtainReadLock();
    try {
      cloneMapFiles = new HashMap<String, List<TabletMapFile>>(mapFiles);
    } finally {
      lock.releaseReadLock();
    }
    
    String fileId = IdGenerator.getId();
      
    //컬럼별로 처리(for 루프 한번에 컬럼 하나 처리)
    for(Map.Entry<String, List<TabletMapFile>> entry : cloneMapFiles.entrySet()) {
      List<TabletMapFile> columnMapFiles = entry.getValue();
      String columnName = entry.getKey();
        
      splitMapFile(midRowKey, createdTablets, columnName, columnMapFiles, fileId);
    } 
  }

  public void splitMapFile( final Row.Key midRowKey, 
                            final Tablet[] createdTablets,
                            final String columnName, 
                            List<TabletMapFile> columnMapFiles, 
                            String fileId) throws IOException {
    
    MapFileReader[] readers = new MapFileReader[columnMapFiles.size()];
    MapFileWriter[] writers = new MapFileWriter[2]; 
    TabletMapFile[] mapFiles = new TabletMapFile[2];
    long targetMapFileSize = 0;

    try {
      for(int i = 0; i < 2; i++) {
        GPath tempMapFilePath = new GPath(Tablet.getTabletSplitTempPath(conf, createdTablets[i].getTabletInfo()), columnName + "/" + fileId + "/");
        mapFiles[i] = new TabletMapFile(conf, CloudataFileSystem.get(conf),
            createdTablets[i].getTabletInfo(), columnName, fileId, tempMapFilePath,
            numOfVersion);
        createdTablets[i].getDiskSSTable().addTabletMapFile(columnName, mapFiles[i]);
        writers[i] = mapFiles[i].getMapFileWriter();
      }
      
      TreeSet<ColumnValueEntry> workPlace = new TreeSet<ColumnValueEntry>(); 
      
      ColumnValue columnValue = null;

      for(int i = 0; i < readers.length; i++) {
        TabletMapFile mapFile = columnMapFiles.get(i);
        targetMapFileSize += mapFile.getDataFileSize();
        
        readers[i] = mapFile.getMapFileReader(Row.Key.MIN_KEY, Row.Key.MAX_KEY);
        columnValue = readers[i].next();
        if(columnValue != null) {
          workPlace.add(new ColumnValueEntry(columnValue, i));
        } else {
          readers[i].close();
          readers[i] = null;
        }
      }
      
      while(true) {
        if(workPlace.isEmpty()) {
          break;
        }
        
        ColumnValueEntry winnerValue = workPlace.pollFirst();
        int winnerIndex = winnerValue.index;
        columnValue = winnerValue.columnValue;

        Row.Key rowKey = columnValue.getRowKey();
        
        int nw = 0;
        if(rowKey.compareTo(midRowKey) > 0) {
          nw = writers[1].write(columnValue);
        } else {
          nw = writers[0].write(columnValue);
        }
        
        if(readers[winnerIndex] != null) {
          columnValue = readers[winnerIndex].next();
          
          if(columnValue != null) {
            workPlace.add(new ColumnValueEntry(columnValue, winnerIndex));
          } else {
            readers[winnerIndex].close();
            readers[winnerIndex] = null;
          }
        }
      } //end of while
    } finally {
      for(int i = 0; i < writers.length; i++) {
        try {
          if(writers[i] != null) {
            writers[i].close();
          }
        } catch (IOException e) {
          LOG.warn("IOException in closing map file writer, exception : " + e);
          for(int j = 0; j < mapFiles.length; j++) {
            mapFiles[j].delete();
          }
          throw e;
        }
      }
      
      LOG.debug(columnName + " file splited from orig: " + targetMapFileSize + " to file1: " 
          + mapFiles[0].getDataFileSize() + " & file2: " + mapFiles[1].getDataFileSize());

      for(int i = 0; i < readers.length; i++) {
        try {
          if(readers[i] != null) {
            readers[i].close();
          }
        } catch (IOException e) {
          throw e;
        }
      }
    }
  }
  
//  private PerformanceRegulator createRegulator(long targetMapFileSize) {
//    return TabletSplitAction.isFastProcessingMode() ?
//        new NullPerformanceRegulator() :
//          new DefaultPerformanceRegulator(targetMapFileSize / 300,
//              TabletSplitAction.getSlowProcessSleepTime());
//  }

  public Row.Key findMidRowKeyForSplit() throws IOException {
    lock.obtainReadLock();
    
    try {
      //MapFile Index의 모든 Row.Key를 merge한다.
      SortedSet<Row.Key> mergedRowKeySet = new TreeSet<Row.Key>();
      int indexRecordCount = 0;
      int mapFileCount = 0;
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        List<TabletMapFile> columnMapFiles = entry.getValue();
        if(columnMapFiles.isEmpty())    continue;
        
        for(TabletMapFile columnMapFile: columnMapFiles) {
          mapFileCount++;
          for(MapFileIndexRecord mapFileIndexRecord: columnMapFile.getMapFileIndexRecords()) {
            mergedRowKeySet.add(mapFileIndexRecord.getRowKey());
            indexRecordCount++;
          }
        }
      }
  
      //Split하기 위해 중간 key를 결정한다.
      int rowRangeSize = mergedRowKeySet.size();
      
      if(rowRangeSize == 1) {
        ///////////////////////////////////////////////////////////////////////////
        //디버그용으로 출력
//        Row.Key previousRowKey = null;
//        for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
//          List<TabletMapFile> columnMapFiles = entry.getValue();
//          if(columnMapFiles.size() == 0)    continue;
//          
//          if(columnMapFiles.size() > 1) {
//            LOG.warn("Map file size > 1 while split:" + tabletInfo.getTabletName());
//          }
//          for(TabletMapFile columnMapFile: columnMapFiles) {
//            for(MapFileIndexRecord mapFileIndexRecord: columnMapFile.getMapFileIndexRecords()) {
//              Row.Key indexRowKey = mapFileIndexRecord.getRowKey();
//              if(previousRowKey != null && !indexRowKey.equals(previousRowKey)) {
//                LOG.debug("Different rowKey:" + indexRowKey);
//              }
//              previousRowKey = indexRowKey;
//            }
//          }
//        }
        //End of 디버그 용
        ///////////////////////////////////////////////////////////////////////////
        
        return null;
      } else if(rowRangeSize == 2) {
        return mergedRowKeySet.first();
      }
      
      int count = 0;
      Row.Key midRowKey = null;
      for(Row.Key eachRowKey: mergedRowKeySet) {
        if(count >= (rowRangeSize / 2)) {
          midRowKey = eachRowKey;
          break;
        }
        count++;
      }
      
      if(midRowKey != null && midRowKey.equals(tabletInfo.getEndRowKey())) {
        LOG.warn("Wrong mid rowkey:");
        for(Row.Key eachRowKey: mergedRowKeySet) {
          LOG.info("MapFile rowKey:" + eachRowKey);
        }
      }
      return midRowKey;
    } finally {
      lock.releaseReadLock();
    }
  }

  public long sumMapFileSize() throws IOException {
    return totalMapFileSize;
  }
  
  public void calculateTotalMapFileSize() {
    long size = 0;
    try {
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        for(TabletMapFile mapFile: entry.getValue()) {
          try {
            size += mapFile.getDataFileSize();
          } catch(java.io.FileNotFoundException e) {
            continue;
          }
        }
      }
      totalMapFileSize = size;
    } catch (Exception e) {
      LOG.warn("Error in calculating total map file size", e);
    }
  }  

  public void moveTempToTablet(boolean deleteParent) throws IOException {
    lock.obtainReadLock();
    try {
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        String columnName = entry.getKey();
        for(TabletMapFile mapFile: entry.getValue()) {
          GPath targetPath = new GPath(Tablet.getTabletPath(conf, tabletInfo), columnName + "/" + mapFile.getFileId() + "/");
          mapFile.moveTo(targetPath, deleteParent);
        }
      }
    } finally {
      lock.releaseReadLock();
    }
  }
  
  /**
   * 테스트 용도로만 사용(동일한 데이터를 각 Column에 저장했을 때 모든 MapFile의 사이즈는 동일해야 한다.
   * @return
   */
  protected boolean checkMapFile() throws IOException {
    long[] sums = new long[mapFiles.size()];
    String[] columnNames = new String[mapFiles.size()];
    int index = 0;
    for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
      columnNames[index] = entry.getKey();
      List<TabletMapFile> columnMapFiles = entry.getValue();
      for(TabletMapFile mapFile: columnMapFiles) {
        sums[index] += mapFile.getDataFileSize();
      }
      index++;
    }
    
    for(int i = 1; i < sums.length; i++) {
      if(sums[i] != sums[i - 1]) {
        LOG.error(tabletInfo.getTabletName() + "," + columnNames[i-1] + "," + columnNames[i] + " different");
        return false;
      }
    }
    
    return true;
  }

  public boolean isEmpty() {
    lock.obtainReadLock();
    try {
      return mapFiles.isEmpty();
    } finally {
      lock.releaseReadLock();
    }      
  }

  public String[] getMapFilePathList() {
    List<String> result = new ArrayList<String>();
    for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
      for(TabletMapFile mapFile: entry.getValue()) {
        result.add(mapFile.getFilePath().toString());
      }
    }
    return result.toArray(new String[]{});
  }
  
  public int[] getMapFileCount() {
    int[] result = new int[mapFiles.size()];
    //lock.obtainReadLock();
    try {
      int indedx = 0;
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        result[indedx++] = entry.getValue().size();
      }
    } finally {
      //lock.releaseReadLock();
    }
    return result;
  }

  public long[] getMapFileSize() throws IOException {
    long[] result = new long[mapFiles.size()];
    //lock.obtainReadLock();
    try {
      int indedx = 0;
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        long sum = 0;
        for(TabletMapFile mapFile: entry.getValue()) {
          try {
            sum += mapFile.getDataFileSize();
          } catch(java.io.FileNotFoundException e) {
            continue;
          }
        }
        result[indedx++] = sum;
      }
    } finally {
      //lock.releaseReadLock();
    }
    return result;
  }
  
  public long[] getMapFileIndexSize() throws IOException {
    long[] result = new long[mapFiles.size()];
    //lock.obtainReadLock();
    try {
      int indedx = 0;
      for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
        long sum = 0;
        for(TabletMapFile mapFile: entry.getValue()) {
          sum += mapFile.getIndexMemorySize();
        }
        result[indedx++] = sum;
      }
    } finally {
      //lock.releaseReadLock();
    }
    return result;
  }
  
  public List<Searchable> getSearchers(Row.Key rowKey, CellFilter cellFilter) throws IOException {
    List<Searchable> mapFileReaders = new ArrayList<Searchable>();
    List<TabletMapFile> tabletMapFileList = null;

    String columnName = cellFilter.getColumnName();
    
    lock.obtainReadLock();
    try {
      if((tabletMapFileList = mapFiles.get(columnName)) == null) {
        return mapFileReaders;
      }
    } finally {
      lock.releaseReadLock();
    }
    
    if(columnMemoryCaches.containsKey(columnName)) {
      //LOG.info(tabletInfo.getTabletName() + " use MemoryCacheSearcher");

      mapFileReaders.add(columnMemoryCaches.get(columnName).getSearcher(rowKey, cellFilter));
    } else {
      for(TabletMapFile mapFile: tabletMapFileList) {
        MapFileReader reader = mapFile.getMapFileReader(rowKey, rowKey, cellFilter);
        mapFileReaders.add(reader);
      }
    }
    return mapFileReaders;
  }
  
  //for test
  public long getMapFileMemorySize()throws IOException {
    long memorySize = 0;
    for(Map.Entry<String, List<TabletMapFile>> entry: mapFiles.entrySet()) {
      for(TabletMapFile mapFile: entry.getValue()) {
        memorySize += mapFile.getIndexMemorySize();
      }
    }
    return memorySize;
  }
  
  
  public String getTestHandlerKey() {
    return tabletServer.getHostName();
  }
  
  public TabletServer getTabletServer() {
    return tabletServer;
  }

  static class MetaValue implements Comparable<MetaValue>{
    ColumnValue columnValue;
    
    MetaValue(ColumnValue columnValue) {
      this.columnValue = columnValue;
    }

    @Override
    public int compareTo(MetaValue obj) {
      int result = columnValue.getRowKey().compareTo(obj.columnValue.getRowKey());
      if(result != 0) {
        return result;
      }
      if(columnValue.getTimestamp() > obj.columnValue.getTimestamp())   return -1;
      else if(columnValue.getTimestamp() == obj.columnValue.getTimestamp()) {
        if(columnValue.isDeleted() == obj.columnValue.isDeleted())  return 0;
        else {
          if(columnValue.isDeleted()) {
            return -1;
          } else {
            return 1;
          }
        }
      }
      else                return 1;
    }
    
    @Override
    public boolean equals(Object obj) {
      if( !(obj instanceof MetaValue) ) {
        return false;
      }
      
      return compareTo((MetaValue)obj) == 0;
    }
  }
  
  public void loadToMemoryCache() throws IOException {
    TableSchema tableScehma = tablet.getTable();
    for(ColumnInfo eachColumnInfo: tableScehma.getColumnInfos()) {
      if(eachColumnInfo.getColumnType() == TableSchema.CACHE_TYPE) {
        String columnName = eachColumnInfo.getColumnName();
        if(notUseColumnMemoryCache.contains(columnName)) {
          continue;
        }
        List<TabletMapFile> tabletMapFiles = mapFiles.get(columnName);
        if(tabletMapFiles == null || tabletMapFiles.isEmpty()) {
          continue;
        }
        
        ColumnCollection columnCollection = new ColumnCollection(conf);
        for(TabletMapFile eachMapFile: tabletMapFiles) {
          MapFileReader reader = eachMapFile.getMapFileReader(Row.Key.MIN_KEY, Row.Key.MAX_KEY);
          ColumnValue columnValue = null;
          while((columnValue = reader.next()) != null) {
            columnCollection.addValue(columnValue.getRowKey(), columnValue, tableScehma.getNumOfVersion());
          }
        }
        setMemoryCache(columnName, columnCollection);
      }
    }
  }
  
  public void setMemoryCache(
      HashMap<String, ColumnCollection> compactingColumnCollections) {
    TableSchema tableScehma = tablet.getTable();

    for(ColumnInfo eachColumnInfo: tableScehma.getColumnInfos()) {
      if(eachColumnInfo.getColumnType() == TableSchema.CACHE_TYPE) {
        String columnName = eachColumnInfo.getColumnName();
        if(compactingColumnCollections.containsKey(columnName)) {
          setMemoryCache(columnName, compactingColumnCollections.get(columnName)); 
        }
      }
    }
  }
  
  private void setMemoryCache(String columnName, ColumnCollection columnCollection) {
    long addedMemorySize = columnCollection.getMemorySize();
    
    if(addedMemorySize == 0 || notUseColumnMemoryCache.contains(columnName)) {
      return;
    }
    
    if(!tablet.getTabletServer().checkCacheCapacity(tabletInfo.getTabletName(), addedMemorySize)) {
      LOG.info(tabletInfo.getTabletName() + " can't use MemoryCache[" + 
          tablet.getTabletServer().currentMemoryCacheSize + ", " + addedMemorySize + "]");
      lock.obtainWriteLock(); 
      try {
        //if can't add, remove cache data
        ColumnMemoryCache cache = columnMemoryCaches.remove(columnName);
        if(cache != null) {
          tabletServer.removeFromMemoryCache(tabletInfo.getTabletName(), cache.getMemorySize());
        }
        notUseColumnMemoryCache.add(columnName);
      } finally {
        lock.releaseWriteLock(); 
      }
      return;
    } else {
      LOG.debug(tabletInfo.getTabletName() + "." + columnName + " use MemoryCache[" + columnCollection.getMemorySize() + " bytes]");
      lock.obtainWriteLock(); 
      try {
        if(columnMemoryCaches.containsKey(columnName)) {
          columnMemoryCaches.get(columnName).addColumnCollection(columnCollection);
        } else {
          columnMemoryCaches.put(columnName, new ColumnMemoryCache(columnCollection));
        }
      } finally {
        lock.releaseWriteLock(); 
      }
    }
  }
  
  public long getColumnMemoryCachesSize() {
    long memorySize = 0;
    for(ColumnMemoryCache eachCache: columnMemoryCaches.values()) {
      memorySize += eachCache.getMemorySize();
    }
    
    return memorySize;
  }
  
  public static void main(String[] args) throws Exception {
    DiskSSTable diskSSTable = new DiskSSTable();
    TabletInfo tabletInfo = new TabletInfo();
    tabletInfo.setTableName("META");
    tabletInfo.setTabletName("META_33211206903548981");
    
    TabletMapFile mapFile1 = new TabletMapFile(new CloudataConf(), tabletInfo, "TabletInfo", "33211281913761034", 0);
    mapFile1.loadIndex();
    TabletMapFile mapFile2 = new TabletMapFile(new CloudataConf(), tabletInfo, "TabletInfo", "33211275611202613", 0);
    mapFile2.loadIndex();
    diskSSTable.addTabletMapFile("TabletInfo", mapFile1);
    diskSSTable.addTabletMapFile("TabletInfo", mapFile2);
    ColumnValue columnValue = diskSSTable.findClosestMeta(new Row.Key("T_TERA_1T.04657695175873701783"), "TabletInfo", true);
    System.out.println(">>>>" + columnValue);
  }
}
