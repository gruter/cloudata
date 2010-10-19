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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.TabletLocationCache;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.master.TableManagerProtocol;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.MapFileColumnValue;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletMapFile;


/**
 * Cloudata에서 관리하는 데이터 파일 및 인덱스 파일을 점검한다. 점검 내용은 다음과 같다.
 * 1. ROOT, META 검증: ROOT, META 데이터의 내용과 실제 서비스되고 있는 Tablet의 내용과 동일한지 확인
 * 2. Index, Data 파일 검증1: Index, Data 파일의 파일 포맷에 대한 검증
 * 3. Index, Data 파일 검증2: Index에서 가리키는 내용과 Data 파일의 내용이 맞는지 검증
 * 4. Index 파일을 이용한 ROOT, META 재구성: ROOT, META corrupt 된 경우 
 *              CommitLog와 Index 파일을 이용하여 복구
 * 5. Index, Data 파일에 대한 통계 정보 생성             
 * 점검과 관련된 작업은 모든 서버가 중지된 상태에서 수행한다.
 * 
 * safe-mode: read, insert 처리 등은 가능하지만 
 *            MinorCompaction, MajorCompatcion, Split 작업은 수행되지 않는다.
 * @author babokim
 *
 */
public class CloudataDataVerifier {
  private static DecimalFormat df = new DecimalFormat("###,###,###,###");
  
  private static final String LEFT_MARGIN = "  ";
  private static final String H_LINE = "---------------------------------------------------------------------------";

  private List<String> params = new ArrayList<String>();
  
  protected ZooKeeper zk;
  
  protected CloudataConf conf;
  
  private List<String> reportMessages = new ArrayList<String>();
  
  private CloudataFileSystem fs;
  
  //Tablet Name -> TabletScanReport
  protected Map<String, TabletScanReport> tabletScanReports = new HashMap<String, TabletScanReport>();
  
  //TabletName -> IndexScanReport
  protected Map<String, IndexScanReport> indexScanReports = new HashMap<String, IndexScanReport>();
  
  
  public CloudataDataVerifier(String[] args) throws Exception {
    for(int i = 0; i < args.length; i++) {
      params.add(args[i]);
    }
    conf = new CloudataConf();
    
    zk = TabletLocationCache.getInstance(conf).getZooKeeper();
    
    fs = CloudataFileSystem.get(conf);
  }
  
  public void printUsage() {
    System.out.println("Usage: ");
    System.out.println("  -meta [-recover] ");
    System.out.println("  -data <table name> [tablet name]");
    System.out.println("  -index <table name> [tablet name]");
    System.out.println("  -statistics <table name>");
  }
  
  protected void addWarn(String message) {
    addMessage("WARN", message);
  }
  
  protected void addError(String message) {
    addMessage("ERROR", message);
  }
  
  protected void addInfo(String message) {
    addMessage("INFO", message);
  }
  
  protected void addMessage(String type, String message) {
    reportMessages.add(type + ": " + message);
  }
  
  public void runCheck() throws Exception {
    if(params.size() == 0) {
      printUsage();
      return;
    }
    String cmd = params.get(0);
    if("-meta".equals(cmd)) {
      if(params.size() > 1 && "-recover".equals(params.get(1))) {
        System.out.println("Recover ROOT/META");
        recoverMeta();
        return;
      } else {
        //ROOT/META 검증
        System.out.println("Checking ROOT/META");
        checkRootMetaData();
        printResult();
      }
    } else if("-data".equals(cmd)) {
      //Data file 검증
      if(params.size() < 2) {
        printUsage();
        return;
      }
      String tableName = params.get(1);
      String tabletName = null;
      if(params.size() >= 3) {
        tabletName = params.get(2);
      }
      DataFileChecker dataFileChecker = new DataFileChecker(this, tableName, tabletName);
      dataFileChecker.checkDataFile();
      printResult();
    } else if("-index".equals(cmd)) {
      //Index 파일 검증
      //Data file 검증
      if(params.size() < 2) {
        printUsage();
        return;
      }
      String tableName = params.get(1);
      String tabletName = null;
      if(params.size() >= 3) {
        tabletName = params.get(2);
      }
      IndexFileChecker indexChecker = new IndexFileChecker(this, tableName, tabletName);
      indexChecker.checkIndexFile();
      printResult();
    } else if("-statistics".equals(cmd)) {
      System.out.println("Not implemented");
    } else {
      printUsage();
      return;
    }
  }
  
  private void printResult() {
    System.out.println(H_LINE);
    if(reportMessages.size() == 0) {
      System.out.println(LEFT_MARGIN + "no report message");
    } else {
      
      int index = 1;
      for(String eachMessage: reportMessages) {
        System.out.println(LEFT_MARGIN + eachMessage);
        index++;
      }
    }
    
//    for(TabletScanReport eachTabletScanReport: tabletScanReports.values()) {
//      eachTabletScanReport.print();
//    }
    System.out.println(H_LINE);
  }
  
  /**
   * 현재의 데이터 파일을 이용하여 ROOT, META를 Recover한다.
   * CommitLogServer는 수행이 되어 있어야 하고 CloudataMaster, TabletServer는 stop 된 상태이어야 함.
   * @throws Exception
   */
  private void recoverMeta() throws Exception {
    String tableName = null;
    if(params.size() > 2) {
      tableName = params.get(2);
    }
    
    MetaRecover recover = new MetaRecover(this, tableName);
    recover.doRecover();
  }
  
  /**
   * ROOT, META 데이터와 실제 서비스되고 있는 Tablet과의 정합성을 검증한다.
   * safe-mode로 설정한 다음 수행된다.
   * 작업 도중 장애가 발생할 경우 강제적으로 safe-mode를 빠져 나오도록 해야 한다.
   * 
   * 1. ROOT의 데이터 파일과 인덱스 파일을 점검한다.
   * 2. CloudataMaster에서 관리하는 ROOT tablet의 정보와 ZooKeeper의 정보가 일치하는지 확인한다.
   * 3. TabletServer에서 서비스 중인 Root Tablet의 메모리 데이터를 점검한다.
   * 4. ROOT 테이블의 데이터 파일을 직접 Scan하여 META Tablet의 정보가 제대로 저장되어 있는지 확인한다.
   * 5. META Tablet에 대해 1 - 4번을 반복한다.(4의 경우 UserTablet에 대한 검증은 하지 않는다)
   *
   */
  private void checkRootMetaData() throws Exception {
    boolean recover = false;
    if(params.size() > 1 && "-recover".equals(params.get(1))) {
      recover = true;
    }
    
    //FIXME: Start Safe-Mode
    try {
      //Check ROOT
      TabletInfo rootTabletInfo = Tablet.getRootTabletInfo(conf, zk);
      
      List<TabletInfo> metaTabletsFromRoot = assertMetaTablet(rootTabletInfo);
      
      if(metaTabletsFromRoot.size() == 0) {
        addError("No META info in ROOT table");
        return;
      }
      
      addInfo("ROOT has " + metaTabletsFromRoot.size() + " META Tablet Infos");
      
      //Check META
      int count = 0;
      for(TabletInfo eachTablet: metaTabletsFromRoot) {
        List<TabletInfo> userTabletsFromMeta = assertMetaTablet(eachTablet);
        count += userTabletsFromMeta.size();
      }
      
      addInfo("META has " + count + " User Tablet Infos");
    } finally {
      //FIXME: End Safe-Mode
    }
  }
  
  /**
   * ROOT table을 복구한다.
   * 복구의 경우 commitlog, data, index 파일을 임시 디렉토리로 이동시킨 후 복구 작업을 수행한다.
   * 복구 작업은 META Tablet의 commitlog, data 파일, memory table 정보 등을 이용하여 복구한다.
   *
   */
  private void recoverRoot() {
    
  }
  
  /**
   * ROOT, META tablet에 대한 검증을 수행한다.
   * @param tabletInfo
   * @return
   * @throws Exception
   */
  private List<TabletInfo> assertMetaTablet(TabletInfo tabletInfo) throws Exception {
    List<TabletInfo> metaTabletInfos = new ArrayList<TabletInfo>();
    
    //check Data, Index file
    DataFileChecker dataFileChecker = new DataFileChecker(this, tabletInfo.getTableName(), null);
    dataFileChecker.checkDataFile();
    IndexFileChecker indexFileChecker = new IndexFileChecker(this, tabletInfo.getTableName(), null);
    indexFileChecker.checkIndexFile();
    
    //Check CloudataMaster's Tablet Info
    try {
      TableManagerProtocol cloudataMaster = CTableManager.getMasterServer(conf);
      TabletInfo[] tabletInfosInMaster = cloudataMaster.getTablets(tabletInfo.getTableName());
      if(tabletInfosInMaster == null || tabletInfosInMaster.length == 0) {
        addError("No Tablet Info in CloudataMaster.");
        return metaTabletInfos;
      } else {
        if(Constants.TABLE_NAME_ROOT.equals(tabletInfosInMaster[0].getTableName())) {
          if(tabletInfosInMaster.length > 1) {
            addError("Too many ROOT Table Infos in CloudataMaster.[" + tabletInfosInMaster.length + "]");
            return metaTabletInfos;
          }
        
          if(!tabletInfo.getTabletName().equals(tabletInfosInMaster[0].getTabletName())) {
            addError("ZooKeeper != CloudataMaster[" + tabletInfo.getTabletName() + "," + 
                tabletInfosInMaster[0].getTabletName() + "]");
            return metaTabletInfos;
          }
          if(!tabletInfo.getAssignedHostName().equals(tabletInfosInMaster[0].getAssignedHostName())) {
            addError("ZooKeeper != CloudataMaster[" + tabletInfo.getAssignedHostName() + "," + 
                tabletInfosInMaster[0].getAssignedHostName() + "]");
            return metaTabletInfos;
          }
        }
      }
    } catch (Exception e) {
      addError("Error occur while checking CloudataMaster's Root Table Info:" + e.getMessage());
      e.printStackTrace();
    }
    
    //Check TabletServer & MemoryTable
    try {
      DataServiceProtocol tabletServer = connectTabletServer(tabletInfo.getAssignedHostName());
      TabletInfo tabletInfosInServer = tabletServer.getTabletInfo(tabletInfo.getTabletName());
      if(tabletInfosInServer == null) {
        addInfo("tablet[" + tabletInfo.getTabletName() + "] not serviced [" + tabletInfo.getAssignedHostName() + "]");
      }
      
      ColumnValue[] columnValues = tabletServer.getAllMemoryValues(tabletInfo.getTabletName(), 
                                          Constants.META_COLUMN_NAME_TABLETINFO);
      if(columnValues != null && columnValues.length > 0) {
        for(ColumnValue eachColumnValue: columnValues) {
          TabletInfo metaTabletInfo = new TabletInfo();
          metaTabletInfo.readFields(eachColumnValue.getValue());
          
          metaTabletInfos.add(metaTabletInfo);
        }
        
        TabletScanReport tabletScanReport = tabletScanReports.get(tabletInfo.getTabletName());
        tabletScanReport.numValueInMemory.put(
            Constants.META_COLUMN_NAME_TABLETINFO,
            tabletScanReport.numValueInMemory.get(
                Constants.META_COLUMN_NAME_TABLETINFO) + columnValues.length);
      }
    } catch (Exception e) {
      addError("Checking TabletServer & MemoryTable:" + e.getMessage());
      e.printStackTrace();
    }
    
    //Scan Data File
    GPath tabletPath = new GPath(Tablet.getTabletPath(conf, tabletInfo), 
        Constants.META_COLUMN_NAME_TABLETINFO);

    GPath[] fileIdPaths = fs.list(tabletPath);
    
    if(fileIdPaths != null && fileIdPaths.length > 0) {
      for(GPath eachPath: fileIdPaths) {
        GPath dataFilePath = new GPath(eachPath, TabletMapFile.DATA_FILE);
        if(!fs.exists(dataFilePath)) {
          continue;
        }
        
        DataInputStream in = fs.openDataInputStream(dataFilePath);
        while(true) {
          MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
          try {
            mapFileColumnValue.readFields(in);
            ColumnValue columnValue = new ColumnValue();
            TabletMapFile.copyColumnValue(mapFileColumnValue, columnValue);
            
            TabletInfo metaTabletInfo = new TabletInfo();
            metaTabletInfo.readFields(columnValue.getValue());
            
            metaTabletInfos.add(metaTabletInfo);
          } catch (EOFException eof) {
            break;
          }
        }
      }
    }
    
    return metaTabletInfos;
  }
  
  /**
   * TabletServer RPC Proxy를 가져온다.
   * @param tabletServerHostName
   * @return
   * @throws IOException
   */
  private DataServiceProtocol connectTabletServer(String tabletServerHostName) throws IOException {
    try {
      return (DataServiceProtocol) CRPC.getProxy(DataServiceProtocol.class,
          DataServiceProtocol.versionID, NetworkUtil.getAddress(tabletServerHostName), conf);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }   
  
  static class IndexScanReport extends TabletScanReport {
    public void print() {
      if(!isPritable()) {
        return;
      }
      
      String leftMargin = LEFT_MARGIN + LEFT_MARGIN;
      System.out.println(H_LINE);
      System.out.println(LEFT_MARGIN + tabletName);
      
      long maxColumnLength = 10;
      for(String eachColumn: columns) {
        if(eachColumn.length() > maxColumnLength) {
          maxColumnLength = eachColumn.length();
        }
      }

      //print header
      String pattern = "";
      for(int i = 0; i < columns.length; i++) {
        pattern += "%" + maxColumnLength + "s | ";
      }
      pattern += "\n";
      
      System.out.printf(leftMargin + "| column         | " + pattern, (Object[])columns);
      
      //print # values in file
      String[] values = new String[columns.length];
      int index = 0;
      for(String eachColumn: columns) {
        values[index++] = df.format(numValueInDataFile.get(eachColumn));
      }
      System.out.printf(leftMargin + "| # IndexRecord  | " +  pattern, (Object[])values);
      
      //print # values in memory
//      index = 0;
//      for(String eachColumn: columns) {
//        values[index++] = df.format(numValueInMemory.get(eachColumn));
//      }
//      System.out.printf("\t # values in memory | " +  pattern, values);

      //print # values in file
      index = 0;
      for(String eachColumn: columns) {
        values[index++] = df.format(numFiles.get(eachColumn));
      }
      System.out.printf(leftMargin + "| # IndexFile    | " +  pattern, (Object[])values);

      //print sum map file length
      index = 0;
      for(String eachColumn: columns) {
        values[index++] = df.format(sumFileSizes.get(eachColumn)) + " bytes";
      }
      System.out.printf(leftMargin + "| IndexFile size | " +  pattern, (Object[])values);
    }
  }
  
  static class TabletScanReport {
    //FIXME average row size
    
    String tabletName;
    String[] columns;
    
    Row.Key startRowKey;
    
    Row.Key endRowKey;
    
    //Column Name -> # of ColumnValues in Data file 
    Map<String, Long> numValueInDataFile = new HashMap<String, Long>();
    
    //Column Name -> # of ColumnValues in Memory
    Map<String, Long> numValueInMemory = new HashMap<String, Long>();
    
    Map<String, Integer> numFiles = new HashMap<String, Integer>();

    Map<String, Long> sumFileSizes = new HashMap<String, Long>();

    public void init(String tabletName, String[] columns) {
      this.tabletName = tabletName;
      this.columns = columns;
      for(String eachColumn: columns) {
        this.numValueInDataFile.put(eachColumn, new Long(0));
        this.numValueInMemory.put(eachColumn, new Long(0));
        this.numFiles.put(eachColumn, new Integer(0));
        this.sumFileSizes.put(eachColumn, new Long(0));
      }
    }

    public void setStartRowKey(Row.Key startRowKey) {
      if(startRowKey == null) {
        return;
      }
      if(this.startRowKey == null) {
        this.startRowKey = new Row.Key(startRowKey.getBytes());
        return;
      } else {
        this.startRowKey = new Row.Key();
      }
      //System.out.println("setStartRow.Key:" + this.startRowKey + "," + startRowKey + "," + this.startRowKey.compareTo(startRowKey));
      if(startRowKey != null && this.startRowKey.compareTo(startRowKey)< 0) {
        this.startRowKey = new Row.Key(startRowKey.getBytes());
      }
    }

    public void setEndRowKey(Row.Key endRowKey) {
      if(endRowKey == null) {
        return;
      }
      if(this.endRowKey != null) {
        this.endRowKey = new Row.Key(endRowKey.getBytes());
        //System.out.println("setEndRow.Key:" + this.endRowKey);
        return;
      } else {
        this.endRowKey = new Row.Key();
      }
      
      //System.out.println("setEndRow.Key:" + this.endRowKey + "," + endRowKey + "," + this.endRowKey.compareTo(endRowKey));
      
      if(endRowKey.getByteSize() > 0 && this.endRowKey.compareTo(endRowKey) > 0) {
        this.endRowKey = new Row.Key(endRowKey.getBytes());
      }
    }

    protected boolean isPritable() {
//      long mapFileSum = 0;
//      for(String eachColumn: columns) {
//        mapFileSum += sumFileSizes.get(eachColumn);
//      }
//      return mapFileSum > 0;
      return true;
    }
    public void print() {
      if(!isPritable()) {
        return;
      }
      
      String leftMargin = LEFT_MARGIN + LEFT_MARGIN;
      System.out.println(H_LINE);
      System.out.println(LEFT_MARGIN + tabletName + "(" + startRowKey + " ~ " + endRowKey + ")");
      
      long maxColumnLength = 10;
      for(String eachColumn: columns) {
        if(eachColumn.length() > maxColumnLength) {
          maxColumnLength = eachColumn.length();
        }
      }

      //print header
      String pattern = "";
      for(int i = 0; i < columns.length; i++) {
        pattern += "%" + maxColumnLength + "s | ";
      }
      pattern += "\n";
      
      System.out.printf(leftMargin + "| column         | " + pattern, (Object[])columns);
      
      //print # values in file
      String[] values = new String[columns.length];
      int index = 0;
      for(String eachColumn: columns) {
        values[index++] = df.format(numValueInDataFile.get(eachColumn));
      }
      System.out.printf(leftMargin + "| # Values(File) | " +  pattern, (Object[])values);
      
      //print # values in memory
//      index = 0;
//      for(String eachColumn: columns) {
//        values[index++] = df.format(numValueInMemory.get(eachColumn));
//      }
//      System.out.printf("\t # values in memory | " +  pattern, values);

      //print # values in file
      index = 0;
      for(String eachColumn: columns) {
        values[index++] = df.format(numFiles.get(eachColumn));
      }
      System.out.printf(leftMargin + "| # MapFile      | " +  pattern, (Object[])values);

      //print sum map file length
      index = 0;
      for(String eachColumn: columns) {
        values[index++] = df.format(sumFileSizes.get(eachColumn)) + " bytes";
      }
      System.out.printf(leftMargin + "| MapFile size   | " +  pattern, (Object[])values);
    }
  }
}
 