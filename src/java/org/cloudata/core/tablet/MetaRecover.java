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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.fs.CommitLogFileSystem;
import org.cloudata.core.fs.CommitLogFileSystemIF;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tabletserver.CommitLog;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.TxId;


public class MetaRecover {
  CloudataDataVerifier verifier;
  ZooKeeper zk;
  CloudataConf conf;
  CloudataFileSystem fs;
  List<String> tableNames = new ArrayList<String>();
  String logImagePath;
  CommitLogFileSystemIF logFs;
  
  String rootTabletName;
  String metaTabletName;
  
  public MetaRecover(CloudataDataVerifier verifier, String tableName) throws Exception {
    this.verifier = verifier;
    this.zk = verifier.zk;
    this.conf = verifier.conf;
    this.fs = CloudataFileSystem.get(conf);
    
    if(tableName != null) {
      tableNames.add(tableName);
    } else {
      GPath[] tablePaths = fs.list(new GPath(conf.get("cloudata.root") + TableSchema.DIR_TABLE));
      if(tablePaths == null || tablePaths.length == 0) {
        System.out.println("No table");
        return;
      }
      
      for(GPath eachTablePath: tablePaths) {
        tableNames.add(eachTablePath.getName());
      }
    }
    logFs = CommitLogFileSystem.getCommitLogFileSystem(conf, null, zk);
  }
  
  public void doRecover() throws Exception {
    //META 삭제
    deleteRootAndMeta();
    List<TabletInfo> allTabletInfos = new ArrayList<TabletInfo>();
    
    for(String eachTableName: tableNames) {
      if(Constants.TABLE_NAME_ROOT.equals(eachTableName) ||
          Constants.TABLE_NAME_META.equals(eachTableName)) {
        continue;
      }
      GPath tablePath = new GPath(TableSchema.getTableDataPath(conf, eachTableName));
      
      GPath[] tabletPaths = fs.list(tablePath);
      if(tabletPaths == null || tabletPaths.length == 0) {
        System.out.println(eachTableName + " no data file[" + tablePath + "]");
        continue;
      }
      
      TableSchema tableSchema = TableSchema.loadTableSchema(conf, zk, eachTableName);
      if(tableSchema == null) {
        System.out.println("No table schema in pleiades[" + eachTableName + "]");
        continue;
      }
      
      List<TabletInfo> tabletInfos = new ArrayList<TabletInfo>();
      
      for(GPath eachPath: tabletPaths) {
        String tabletName = eachPath.getName(); 
        TabletInfo tabletInfo = getTabletData(tableSchema, tabletName);
        if(tabletInfo != null) {
          tabletInfos.add(tabletInfo);
        }
      }
      
      if(tabletInfos.size() == 0) {
        //Tablet이 하나도 없는 경우 빈 Tablet을 생성한다.
        String tabletName = Tablet.generateTabletName(eachTableName);
        TabletInfo tabletInfo = new TabletInfo(eachTableName, tabletName, Row.Key.MIN_KEY, Row.Key.MAX_KEY);
        tabletInfos.add(tabletInfo);
      } else {
        Collections.sort(tabletInfos);
        TabletInfo lastTabletInfo = tabletInfos.remove(tabletInfos.size() - 1);
        lastTabletInfo.setEndRowKey(Row.Key.MAX_KEY);
        tabletInfos.add(lastTabletInfo);
      }
      
      allTabletInfos.addAll(tabletInfos);
    }
    
    //META에 추가한다.
    TabletInfo metaTabletInfo = new TabletInfo(Constants.TABLE_NAME_META, metaTabletName, Row.Key.MIN_KEY, Row.Key.MAX_KEY);
    
    int index = 1;
    logFs.open(metaTabletInfo.getTabletName(), true);
    System.out.println("-----------------------------------------------------");
    for(TabletInfo userTabletInfo: allTabletInfos) {
      Row.Key rowKey = Tablet.generateMetaRowKey(userTabletInfo.getTableName(), userTabletInfo.getEndRowKey());
      TxId txId = TxId.generate(metaTabletInfo.getTabletName(), true);
      
      long timestamp = System.currentTimeMillis();
      CommitLog commitLog = new CommitLog(Constants.LOG_OP_ADD_COLUMN_VALUE, 
          metaTabletInfo.getTableName(), 
          rowKey,
          Constants.META_COLUMN_NAME_TABLETINFO,
          new Cell.Key(userTabletInfo.getTabletName()),
          timestamp,
          userTabletInfo.getWriteBytes());     
      logFs.addCommitLog(metaTabletInfo.getTabletName(), txId.getTxId(), index, commitLog);  
      index++;
      
      System.out.println("UserTablet insert into META:" + userTabletInfo);
    }
    logFs.close(metaTabletName, true);

    System.out.println("-----------------------------------------------------");
    
    //ROOT에 META를 추가한다.
    TabletInfo rootTabletInfo = new TabletInfo(Constants.TABLE_NAME_ROOT, rootTabletName, Row.Key.MIN_KEY, Row.Key.MAX_KEY);
    
    logFs.open(rootTabletInfo.getTabletName(), true);
    Row.Key rowKey = Tablet.generateMetaRowKey(metaTabletInfo.getTableName(), metaTabletInfo.getEndRowKey());
    TxId txId = TxId.generate(rootTabletInfo.getTabletName(), true);
      
    long timestamp = System.currentTimeMillis();
    if(metaTabletInfo.getTabletName() == null) {
      throw new IOException("META tablet name is null:" + metaTabletInfo);
    }
    CommitLog commitLog = new CommitLog(Constants.LOG_OP_ADD_COLUMN_VALUE, 
        metaTabletInfo.getTableName(), 
        rowKey,
        Constants.META_COLUMN_NAME_TABLETINFO,
        new Cell.Key(metaTabletInfo.getTabletName()),
        timestamp,
        metaTabletInfo.getWriteBytes());     
    logFs.addCommitLog(rootTabletInfo.getTabletName(), txId.getTxId(), index, commitLog);  
    logFs.close(rootTabletName, true);
    
    System.out.println("Complete ROOT & META Recovering");
    System.out.println("\tROOT: " + rootTabletName);
    System.out.println("\tMETA: " + metaTabletName);
  }
  
  private TabletInfo getTabletData(TableSchema tableSchema, String tabletName) throws Exception { 
    try {
      String[] columns = tableSchema.getColumnsArray();
      
      GPath tabletPath = Tablet.getTabletPath(conf, tableSchema.getTableName(), tabletName);
      
      Row.Key endRowKey = Row.Key.MIN_KEY;
      
      TabletInfo tabletInfo = new TabletInfo(tableSchema.getTableName(), tabletName, Row.Key.MIN_KEY, Row.Key.MIN_KEY);
      for(String eachColumn: columns) {
        GPath[] fileIds = fs.list(new GPath(tabletPath, eachColumn));
        
        int fileLen = fileIds == null ? 0 : fileIds.length;
        
        if(fileLen == 0) {
          continue;
        }
        
        //파일 내용 Read
        for(GPath eachFilePath: fileIds) {
          GPath dataFilePath = new GPath(eachFilePath, TabletMapFile.DATA_FILE);
          DataInputStream din = fs.openDataInputStream(dataFilePath);
          
          long currentPos = 0;
          Row.Key rowKey = new Row.Key();
          
          try {
            while(true) {
              try {
                currentPos = DataFileChecker.assertColumnValue(verifier, din, dataFilePath, currentPos, rowKey);
                if(currentPos < 0) {
                  return null;
                }
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
      } //end of column loop
      
      //set end Row.Key
      if(endRowKey.equals(Row.Key.MIN_KEY)) {
        return null;
      }
      tabletInfo.setEndRowKey(endRowKey);
      return tabletInfo;
      
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private InetSocketAddress[] getServersFromImage(String tabletName) throws IOException {
    GPath commitLogMetaPath = new GPath(logImagePath + "/" + tabletName);
    if(!fs.exists(commitLogMetaPath)) {
      return null;
    } else {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(commitLogMetaPath)));
      String line = null;
      
      List<InetSocketAddress> commitLogServerInfo = new ArrayList<InetSocketAddress>();
      while( (line = reader.readLine()) != null) {
        commitLogServerInfo.add(NetworkUtil.getAddress(line));      
      }
      reader.close();
      return commitLogServerInfo.toArray(new InetSocketAddress[]{});
    }
  }
  
  private void deleteRootAndMeta() throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    for(String eachTableName: tableNames) {
      if(!Constants.TABLE_NAME_ROOT.equals(eachTableName) &&
          !Constants.TABLE_NAME_META.equals(eachTableName)) {
        continue;
      }
      
      //FileSystem에 있는 내용 확인
      GPath tablePath = new GPath(TableSchema.getTableDataPath(conf, eachTableName));
      GPath[] tabletPaths = fs.list(tablePath);
      if(tabletPaths == null || tabletPaths.length == 0) {
        continue;
      }
      
      for(GPath eachTabletPath: tabletPaths) {
        String tabletName = eachTabletPath.getName();
        
        System.out.print("Found META,ROOT Tablet[" + tabletName + "]. Delete all datas?(y/n) :");
        String line = reader.readLine();
        if(!line.equals("y")) {
          continue;
        } 
          
        //META, ROOT 삭제
        System.out.println("Deleting META,ROOT Tablet[" + tabletName + "][" + eachTabletPath + "]");
        fs.delete(eachTabletPath, true);
        logFs.delete(tabletName);
        
        if(Constants.TABLE_NAME_ROOT.equals(eachTableName)) {
          rootTabletName = tabletName;
          fs.mkdirs(eachTabletPath);
        } else if(Constants.TABLE_NAME_META.equals(eachTableName)) {
          if(metaTabletName == null) {
            metaTabletName = tabletName;
            fs.mkdirs(eachTabletPath);
          }
        }
      }
    }
    
    reader.close();
  }
}
