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
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.master.TableManagerProtocol;
import org.cloudata.core.tablet.CloudataDataVerifier.TabletScanReport;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;


/**
 * Tablet의 데이터 파일을 검증한다.
 * @author babokim
 *
 */
class DataFileChecker {
  private ZooKeeper zk;
  
  private CloudataConf conf;
  
  private CloudataFileSystem fs;
  
  private String tableName;
  
  private String tabletName;
  
  private CloudataDataVerifier verifier;
  
  private CTable ctable;
  
  public DataFileChecker(CloudataDataVerifier verifier, 
                          String tableName, String tabletName) throws Exception {
    this.conf = verifier.conf;
    this.fs = CloudataFileSystem.get(conf);
    this.zk = verifier.zk;
    this.tableName = tableName;
    this.tabletName = tabletName;
    this.verifier = verifier;
    try {
      this.ctable = CTable.openTable(conf, tableName);
    } catch (Exception e) {
      System.out.println("Can't open NTable. so can't compare end rowkey");
    }
  }
  
  /**
   * 데이터 파일을 검증한다.
   * 데이터 파일 검증을 위해 CloudataMaster에서 관리하는 정보와 비교하여 사용되지 않는 Tablet Path를 찾아 
   * 리포팅한다.(사용하지 않는다고 리포팅 된 경우에도 신중하세 처리할 필요가 있음)
   * @param tableName
   * @param tabletName
   * @return
   */
  protected void checkDataFile() throws IOException {
    TableSchema tableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
    if(tableSchema == null) {
      verifier.addError("No table schema in pleiades[" + tableName + "]");
      return;
    }
    
    if(tabletName != null && tabletName.length() > 0) {
      checkTabletDataFile(tableName, tabletName);
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
        checkTabletDataFile(tableName, eachTabletInfo.getTabletName());
      }
      
      //CloudataMaster의 Tablet 정보에 없는 경우 일단 검증은 하지만 CloudataMaster와 맞지 않는 내용을 report 한다.
      for(String eachTabletName: fsTabletNames) {
        verifier.addWarn(eachTabletName + " dir exists in file system, but no tablet info in CloudataMaster");  
        checkTabletDataFile(tableName, eachTabletName);
      }
    }
  }
  
  /**
   * Tablet에 대한 데이터 파일을 검증한다.
   * @param tabletName
   * @return read한 column record 갯수
   */
  private void checkTabletDataFile(String tableName, String tabletName) throws IOException {
    int totalFileCount = 0;

    try {
      TableSchema tableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
      if(tableSchema == null) {
        verifier.addError("No table schema in pleiades[" + tableName + "]");
        return;
      }
      String[] columns = tableSchema.getColumnsArray();
      
      TabletScanReport tabletScanReport = verifier.tabletScanReports.get(tabletName);
      if(tabletScanReport == null) {
        tabletScanReport = new TabletScanReport();
        tabletScanReport.init(tabletName, columns);
        verifier.tabletScanReports.put(tabletName, tabletScanReport);
      }
      
      GPath tabletPath = Tablet.getTabletPath(conf, tableName, tabletName);
      
      Row.Key startRowKey = null;
      Row.Key endRowKey = null;
      
      for(String eachColumn: columns) {
        GPath[] fileIds = fs.list(new GPath(tabletPath, eachColumn));
        
        int fileLen = fileIds == null ? 0 : fileIds.length;
        tabletScanReport.numFiles.put(eachColumn, tabletScanReport.numFiles.get(eachColumn) + fileLen);
        
        if(fileLen == 0) {
          continue;
        }
        totalFileCount += fileLen;
        
        //집계 정보 수정
        long numValues = 0;
        long sumFileLength = 0;
        
        //파일 내용 Read
        for(GPath eachFilePath: fileIds) {
          GPath dataFilePath = new GPath(eachFilePath, TabletMapFile.DATA_FILE);
          sumFileLength += fs.getLength(dataFilePath);
          DataInputStream din = fs.openDataInputStream(dataFilePath);
          
          long currentPos = 0;
          Row.Key rowKey = new Row.Key();
          
          int index = 0;
          try {
            while(true) {
              try {
                currentPos = assertColumnValue(verifier, din, dataFilePath, currentPos, rowKey);
                
                //MapFile에서 첫번째 읽은 값을 이용하여 startRowKey 설정
                if(index == 0) {
                  if(startRowKey == null) {
                    startRowKey = new Row.Key(rowKey.getBytes());
                  }
                  
                  if(rowKey.compareTo(startRowKey) < 0) {
                    startRowKey = new Row.Key(rowKey.getBytes());
                  }
                  index++;
                }
                if(currentPos < 0) {
                  return;
                }
                numValues++;
              } catch (EOFException eof) {
                if(endRowKey == null) {
                  endRowKey = new Row.Key(rowKey.getBytes());
                }
                if(rowKey.compareTo(endRowKey) > 0) {
                  endRowKey = new Row.Key(rowKey.getBytes());
                }
                break;
              } catch (IOException e) {
                verifier.addError("Error while checkTabletDataFile:" + e.getMessage());
              }
            } //end of read while loop
          } finally {
            if(din != null) {
              din.close();
            }
          }
        } //end of map file loop
        
        tabletScanReport.numValueInDataFile.put(eachColumn, tabletScanReport.numValueInDataFile.get(eachColumn) + numValues);
        tabletScanReport.sumFileSizes.put(eachColumn, tabletScanReport.sumFileSizes.get(eachColumn) + sumFileLength);
      } //end of column loop
      tabletScanReport.setStartRowKey(startRowKey);
      tabletScanReport.setEndRowKey(endRowKey);

    } finally {
      TabletScanReport tabletScanReport = verifier.tabletScanReports.get(tabletName);   
      if(tabletScanReport != null && tabletScanReport.isPritable()) {
        if(ctable != null) {
          TabletInfo tabletInfo = ctable.getTabletInfo(tabletName);
          if(tabletInfo != null) {
            if(tabletScanReport.endRowKey != null) {
              if(tabletScanReport.endRowKey.compareTo(tabletInfo.getEndRowKey()) > 0) {
                verifier.addError("TabletInfo.endRowKey(" +  tabletInfo.getEndRowKey() + 
                    ") != DataFile.endRowKey(" + tabletScanReport.endRowKey +"), Tablet=" + tabletName);
              } else if(tabletScanReport.endRowKey.compareTo(tabletInfo.getEndRowKey()) < 0) {
                verifier.addWarn("TabletInfo.endRowKey(" +  tabletInfo.getEndRowKey() + 
                    ") != DataFile.endRowKey(" + tabletScanReport.endRowKey +"), Tablet=" + tabletName);
              }
            }
          }
        }
        tabletScanReport.print();
      }
    }
  }
  
  /**
   * DFS에 저장된 데이터파일을 직접 오픈하여 Column Value 포맷이 정상적으로 저장되어 있는지 검증한다.
   * @param din
   * @param dataFilePath
   * @param currentPos
   * @return
   * @throws IOException
   */
  public static long assertColumnValue(CloudataDataVerifier verifier,
      DataInputStream din, GPath dataFilePath, long currentPos, Row.Key rowKey) throws IOException {
    //<Row.Key>
    int length = CWritableUtils.readVInt(din);
    if(Math.abs(length) > Constants.MAX_KEY_LENGTH) {
      verifier.addError("Row.Key length too long[length=" + length + ",pos=" + 
          currentPos + ",path="+ dataFilePath + "]. stop scanning.");
      return -1;
    }
    byte[] rowKeyBytes = new byte[length];
    din.readFully(rowKeyBytes, 0, length);
    currentPos += (CWritableUtils.getVIntSize (length) + length);
    //</Row.Key>
    rowKey.set(rowKeyBytes);
    
    //<Cell.Key>
    length = CWritableUtils.readVInt(din);
    if(Math.abs(length) > Constants.MAX_KEY_LENGTH) {
      verifier.addError("Cell.Key length too long[length=" + length + ",pos=" + 
          currentPos + ",path="+ dataFilePath + "]. stop scanning.");
      return -1;
    }
    byte[] columnKeyBytes = new byte[length];
    din.readFully(columnKeyBytes, 0, length);
    currentPos += (CWritableUtils.getVIntSize (length) + length);
    //</Cell.Key>
    
    //<OpCode>
    int opCode = din.readInt();
    if(opCode != Constants.DELETEED && 
        opCode != Constants.INSERTED) {
      verifier.addError("Wrong record operation code(DELETEED or INSERTED): code=" + opCode + ",pos=" + 
          currentPos + ",path="+ dataFilePath + "]. stop scanning.");
      return -1;
    }
    currentPos += CWritableUtils.getIntByteSize(); 
    //<//OpCode>

    //<Timestamp>
    long timestamp = din.readLong();
    new Date(timestamp);
    currentPos += CWritableUtils.getLongByteSize();
    //</Timestamp>

    //<Value>
    int valueLength = din.readInt();
    currentPos += CWritableUtils.getIntByteSize(); 
    if(Math.abs(valueLength) > 10 * 1024 * 1024) { //100MB
      verifier.addError("Value length too long[length=" + length + ",pos=" + 
          currentPos + ",path="+ dataFilePath + "]");
    }

    if(valueLength > 0) {
      byte[] value = new byte[valueLength];
      din.readFully(value);
      currentPos += valueLength;
    } 
    currentPos += CWritableUtils.getIntByteSize();  
    //</Value>

    return currentPos;
  } 
}
