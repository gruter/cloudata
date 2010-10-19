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
package org.cloudata.core.tablet;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.master.TableManagerProtocol;
import org.cloudata.core.tablet.CloudataDataVerifier.IndexScanReport;
import org.cloudata.core.tabletserver.MapFileColumnValue;
import org.cloudata.core.tabletserver.MapFileIndexRecord;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;


class IndexFileChecker {
  private ZooKeeper zk;
  
  private CloudataConf conf;
  
  private CloudataFileSystem fs;
  
  private String tableName;
  
  private String tabletName;
  
  private CloudataDataVerifier verifier;
  
  public IndexFileChecker(CloudataDataVerifier verifier, 
                          String tableName, String tabletName) throws Exception {
    this.conf = verifier.conf;
    this.fs = CloudataFileSystem.get(conf);
    this.zk = verifier.zk;
    this.tableName = tableName;
    this.tabletName = tabletName;
    this.verifier = verifier;
  }
  
  protected void checkIndexFile() throws IOException {
    TableSchema tableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
    if(tableSchema == null) {
      verifier.addError("No table schema in pleiades[" + tableName + "]");
      return;
    }
    
    if(tabletName != null && tabletName.length() > 0) {
      checkTabletIndexFile(tableName, tabletName);
    } else {
      GPath tablePath = new GPath(TableSchema.getTableDataPath(conf, tableName));
      
      GPath[] tabletPaths = fs.list(tablePath);
      if(tabletPaths == null || tabletPaths.length == 0) {
        verifier.addInfo(tableName + " no Data File[" + tablePath + "]");
        return;
      }

      //사용하지 않는 디렉토리가 존재할 경우 reporting 한다.
      TableManagerProtocol cloudataMaster = CTableManager.getMasterServer(conf);
      TabletInfo[] tabletInfosInMaster = cloudataMaster.getTablets(tableName);
      
      if(tabletInfosInMaster == null || tabletInfosInMaster.length == 0) {
        verifier.addError("No Tablet Info of " + tableName + " in CloudataMaster");
        return;
      }
      
      Set<String> fsTabletNames = new HashSet<String>();
      for(GPath eachPath: tabletPaths) {
        fsTabletNames.add(eachPath.getName());
      }
      
      //CloudataMaster에 있는 Tablet 정보를 기준으로 하여 Data File을 확인
      for(TabletInfo eachTabletInfo: tabletInfosInMaster) {
        fsTabletNames.remove(eachTabletInfo.getTabletName());
        checkTabletIndexFile(tableName, eachTabletInfo.getTabletName());
      }
      
      //CloudataMaster의 Tablet 정보에 없는 경우 일단 검증은 하지만 CloudataMaster와 맞지 않는 내용을 report 한다.
      for(String eachTabletName: fsTabletNames) {
        verifier.addWarn(eachTabletName + " dir exists in file system, but no tablet inof in CloudataMaster");  
        checkTabletIndexFile(tableName, eachTabletName);
      }
    }
  }
  
  private void checkTabletIndexFile(String tableName, String tabletName) throws IOException {
    try {
      TableSchema tableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
      if(tableSchema == null) {
        verifier.addError("No table schema in pleiades[" + tableName + "]");
        return;
      }
      String[] columns = tableSchema.getColumnsArray();
      
      IndexScanReport indexScanReport = verifier.indexScanReports.get(tabletName);
      if(indexScanReport == null) {
        indexScanReport = new IndexScanReport();
        indexScanReport.init(tabletName, columns);
        verifier.indexScanReports.put(tabletName, indexScanReport);
      }
      
      GPath tabletPath = Tablet.getTabletPath(conf, tableName, tabletName);
      
      for(String eachColumn: columns) {
        GPath[] fileIds = fs.list(new GPath(tabletPath, eachColumn));
        if(fileIds == null || fileIds.length == 0) {
          continue;
        }
        
        //집계 정보 수정
        indexScanReport.numFiles.put(eachColumn, indexScanReport.numFiles.get(eachColumn) + fileIds.length);
        
        long numIndexValues = 0;
        long sumFileLength = 0;
        
        //파일 내용 Read
        for(GPath eachFilePath: fileIds) {
          GPath dataFilePath = new GPath(eachFilePath, TabletMapFile.DATA_FILE);
          long dataFileLength = fs.getLength(dataFilePath);

          GPath indexFilePath = new GPath(eachFilePath, TabletMapFile.IDX_FILE);
          sumFileLength += fs.getLength(indexFilePath);
          
          DataInputStream indexIn = new DataInputStream(fs.open(indexFilePath));
          DataInputStream dataIn = fs.openDataInputStream(dataFilePath);
          
          MapFileIndexRecord mapFileIndexRecord = new MapFileIndexRecord();
          MapFileIndexRecord previousIndexRecord = new MapFileIndexRecord();
          
          long currentPos = 0;
          try {
            while(true) {
              long pos = assertIndex(indexIn, indexFilePath, dataFileLength, currentPos, mapFileIndexRecord);
              if(pos < 0) {
                return;
              }
              
              verifyIndexRecord(dataIn, dataFilePath, mapFileIndexRecord, previousIndexRecord, indexFilePath, currentPos);
              
              currentPos = pos;
              previousIndexRecord.setValue(mapFileIndexRecord.getRowKey().getBytes(), 
                  mapFileIndexRecord.getCellKey().getBytes(), 
                  mapFileIndexRecord.getTimestamp(), 
                  mapFileIndexRecord.getOffset());
              
              numIndexValues++;
            }
          } catch (EOFException eof) {
            
          } finally {
            indexIn.close();
            dataIn.close();
          }
        } //end of map file loop
        
        indexScanReport.numValueInDataFile.put(eachColumn, indexScanReport.numValueInDataFile.get(eachColumn) + numIndexValues);
        indexScanReport.sumFileSizes.put(eachColumn, indexScanReport.sumFileSizes.get(eachColumn) + sumFileLength);
      } //end of column loop
    } finally {
      IndexScanReport indexScanReport = verifier.indexScanReports.get(tabletName);      
      if(indexScanReport != null) {
        indexScanReport.print();
      }
    }
  }

  private long assertIndex(DataInputStream in, GPath indexFilePath, long dataFileLength,
      long currentPos, MapFileIndexRecord mapFileIndexRecord) throws IOException {
    
    //<Row.Key>
    int length = CWritableUtils.readVInt(in);
    if(Math.abs(length) > Constants.MAX_KEY_LENGTH) {
      verifier.addError("Row.Key length too long[length=" + length + ",pos=" + 
          currentPos + ",path="+ indexFilePath + "]. stop scanning.");
      return -1;
    }
    byte[] rowKey = new byte[length];
    in.readFully(rowKey, 0, length);
    currentPos += (CWritableUtils.getVIntSize (length) + length);
    //</Row.Key>
    
    //<Cell.Key>
    length = CWritableUtils.readVInt(in);
    if(Math.abs(length) > Constants.MAX_KEY_LENGTH) {
      verifier.addError("Cell.Key length too long[length=" + length + ",pos=" + 
          currentPos + ",path="+ indexFilePath + "]. stop scanning.");
      return -1;
    }    
    byte[] columnKey = new byte[length];
    in.readFully(columnKey, 0, length);
    currentPos += (CWritableUtils.getVIntSize (length) + length);
    //</Cell.Key>
    
    //<Timestamp>
    long timestamp = in.readLong();
    new Date(timestamp);
    //</Timestamp>
    
    //<offset>
    long offset = in.readLong();
    if(offset >= dataFileLength) {
      verifier.addError("Offset exceeds data file length[offset=" + offset + ",pos=" + 
          currentPos + ",dataFileLength=" + dataFileLength + ",path="+ indexFilePath + "]. stop scanning.");
      return -1;
    }
    //</offset>
    
    
    mapFileIndexRecord.setValue(rowKey, columnKey, timestamp, offset);
    
    return currentPos; 
  }
  
  
  private boolean verifyIndexRecord(DataInputStream dataIn, 
      GPath dataFilePath, 
      MapFileIndexRecord mapFileIndexRecord,
      MapFileIndexRecord previousIndexRecord,
      GPath indexFilePath,
      long currentPos) throws IOException {
    fs.seek(dataIn, mapFileIndexRecord.getOffset());
    
    MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
    try {
      mapFileColumnValue.readFields(dataIn);
      
      MapFileColumnValue previousColumnValue = new MapFileColumnValue();
      if(previousIndexRecord.getRowKey() != null &&
          previousIndexRecord.getRowKey().getByteSize() > 0) {
        previousColumnValue.setRowKey(previousIndexRecord.getRowKey());
        previousColumnValue.setCellKey(previousIndexRecord.getCellKey());
        previousColumnValue.setTimestamp(previousIndexRecord.getTimestamp());
      }
      
      MapFileColumnValue maxColumnValue = new MapFileColumnValue();
      maxColumnValue.setRowKey(mapFileIndexRecord.getRowKey());
      maxColumnValue.setCellKey(mapFileIndexRecord.getCellKey());
      maxColumnValue.setTimestamp(mapFileIndexRecord.getTimestamp());
      
      //Data file에서 읽은 값이 
      if(mapFileColumnValue.compareTo(previousColumnValue) <= 0 || 
          mapFileColumnValue.compareTo(maxColumnValue) > 0) {
          verifier.addError("Column Value in data file must exist in between [" + 
              previousColumnValue + "] and [" + maxColumnValue +"] ,pos=" + 
              currentPos + ",path="+ indexFilePath + "]. stop scanning."); 
      return false;
      }
    } catch (Exception e) {
      verifier.addError("verifyIndexRecord: pos=" + 
            currentPos + ",path="+ indexFilePath + "]. stop scanning.:" + e.getMessage()); 
      return false;
    }
    
    return true;
  }
}
