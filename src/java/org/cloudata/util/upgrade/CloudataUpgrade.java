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
package org.cloudata.util.upgrade;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritableComparable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.master.CloudataMaster;
import org.cloudata.core.master.TableManagerProtocol;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.MapFileColumnValue;
import org.cloudata.core.tabletserver.MapFileIndexRecord;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TabletManagerProtocol;
import org.cloudata.core.tabletserver.TabletMapFile;
import org.cloudata.core.tabletserver.TabletServerInfo;
import org.cloudata.core.tabletserver.ValueCollection;


/**
 * @author jindolk
 * 
 */
public class CloudataUpgrade {
  // upgrade 1.3 -> 1.4
  // 1. modify TabletManagerProtocol
  // 2. modify TabletServer(add doAllTabletMinorCompaction)
  // 3. add CloudataUpgrade class
  // 4. build, cluster restart
  // 5. java CloudataUpgrade minor
  // 6. check memory empty
  // 7. java CloudataUpgrade major
  // 8. stop cluster
  // move to cloudata-1.4
  // 9. java CloudataUpgrade meta
  //10. backup ROOT, META file in DFS
  //11. replace local MapFie
  //12. java TabletMapFile 
  //13. start cluster(new version)

  static ZooKeeper zk;

  static CloudataConf conf;

  static Log LOG = LogFactory.getLog(CloudataUpgrade.class);

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("Usage: java CloudataUpgrade <minor|major|meta|rollback>");
      System.exit(0);
    }

    conf = new CloudataConf();
    zk = LockUtil.getZooKeeper(conf, "CloudataClient_Upgrade_"
        + System.currentTimeMillis(), null);
    if ("minor".equals(args[0])) {
      doAllTabletMinorCompaction();
    } else  if ("major".equals(args[0])) {
      doMetaMajorCompaction();
    } else if ("meta".equals(args[0])) {
      modifyMetaMapFile();
    } else if ("rollback".equals(args[0])) {
      rollback();
    } else {
      System.out.println("Usage: java CloudataUpgrade <minor|meta>");
      System.exit(0);
    }
  }

  private static void rollback() throws IOException {
    TabletInfo14 rootTablet14 = getRootTabletInfo14(conf, zk);
    
    LOG.info("====== Modify ROOT TabletInfo in ZooKeeper: " + LockUtil.getZKPath(conf, Constants.ROOT_TABLET));
    TabletInfo13 rootTablet13 = rootTablet14.getTabletInfo13();
    
    try {
      zk.setData(LockUtil.getZKPath(conf, Constants.ROOT_TABLET), rootTablet13.getWriteBytes(), -1);
    } catch (KeeperException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    }
  }
  
  private static void doAllTabletMinorCompaction() throws IOException {
    LOG.info("Start doAllTabletMinorCompaction");
    TableManagerProtocol masterServer = (TableManagerProtocol) CRPC.getProxy(
        TableManagerProtocol.class, TableManagerProtocol.versionID, NetworkUtil
            .getAddress(CloudataMaster.getMasterServerHostName(conf, zk)), conf);

    TabletServerInfo[] tabletServers = masterServer.getTabletServerInfos();
    if (tabletServers == null) {
      LOG.warn("No TabletServers");
      System.exit(0);
    }

    for (TabletServerInfo tabletServerInfo : tabletServers) {
      TabletManagerProtocol tabletServer = (TabletManagerProtocol) CRPC
          .getProxy(TabletManagerProtocol.class,
              TabletManagerProtocol.versionID, NetworkUtil
                  .getAddress(tabletServerInfo.getHostName()), conf);
      tabletServer.doAllTabletMinorCompaction();
      LOG.info("Call MinorCompaction to " + tabletServerInfo.getHostName());
    }
    LOG.info("End doAllTabletMinorCompaction");
  }

  private static void doMetaMajorCompaction() throws IOException {
    LOG.info("Start doMetaMajorCompaction");
    TabletInfo rootTablet = Tablet.getRootTabletInfo(conf, zk);
    
    TableScanner scanner = ScannerFactory.openScanner(conf, rootTablet, Constants.META_COLUMN_NAME_TABLETINFO);
    Row row = null;
    while( (row = scanner.nextRow()) != null ) {
      Cell cell = row.getFirst(Constants.META_COLUMN_NAME_TABLETINFO);
      TabletInfo tabletInfo = new TabletInfo();
      tabletInfo.readFields(cell.getBytes());
      
      LOG.info("majorCompaction: " + tabletInfo.getTabletName());
      TabletManagerProtocol tabletServer = (TabletManagerProtocol) CRPC
      .getProxy(TabletManagerProtocol.class,
          TabletManagerProtocol.versionID, NetworkUtil
              .getAddress(tabletInfo.getAssignedHostName()), conf);
      
      tabletServer.doActionForTest(tabletInfo.getTabletName(), "majorCompaction");
    }
    LOG.info("majorCompaction: " + rootTablet.getTabletName());
    TabletManagerProtocol tabletServer = (TabletManagerProtocol) CRPC
    .getProxy(TabletManagerProtocol.class,
        TabletManagerProtocol.versionID, NetworkUtil
            .getAddress(rootTablet.getAssignedHostName()), conf);
    
    tabletServer.doActionForTest(rootTablet.getTabletName(), "majorCompaction");
    LOG.info("End doMetaMajorCompaction");
  }
  
  private static void modifyMetaMapFile() throws IOException {
    LOG.info("====== Start modifyMetaMapFile");

    try {
//      TabletInfo rootTablet14 = Tablet.getRootTabletInfo(conf, zk);
//      TabletInfo13 rootTablet = new TabletInfo13();
//      rootTablet.tableName = rootTablet14.getTableName();
//      rootTablet.tabletName = rootTablet14.getTabletName();
//      rootTablet.endRowKey = rootTablet14.getEndRowKey();
//      rootTablet.assignedHostName = rootTablet14.getAssignedHostName();
      
      TabletInfo13 rootTablet = getRootTabletInfo(conf, zk);
      if (rootTablet == null) {
        throw new IOException("Root Tablet is : " + rootTablet);
      }

      //////////////////////////////////////////////////////////////////////
      //ROOT Tablet
      List<MapFileColumnValue> mapFileColumnValues = getTabletInfos(rootTablet);
      if(mapFileColumnValues.size() == 0) {
        LOG.warn("No META TableInfo in ROOT Tablet:" + rootTablet);
        return;
      }
      
      SortedSet<TabletInfo13> metaTablets = new TreeSet<TabletInfo13>();
      UpgradeFileWriter writer = new UpgradeFileWriter(rootTablet.tabletName);
      try {
        Row.Key startRowKey = Row.Key.MIN_KEY;
        String previousTableName = null;
        for(MapFileColumnValue eachColumnValue: mapFileColumnValues) {
          if(eachColumnValue.isDeleted()) {
            writer.write(eachColumnValue);
          } else {
            TabletInfo13 tabletInfo13 = new TabletInfo13();
            tabletInfo13.readFields(eachColumnValue.getValue());
            TabletInfo14 tabletInfo = tabletInfo13.getTabletInfo();
            
            if(previousTableName != null && !previousTableName.equals(tabletInfo13.tableName)) {
              startRowKey = Row.Key.MIN_KEY;
            } 
            tabletInfo.startRowKey = startRowKey;
            
            Row.Key rowKey = Tablet.generateMetaRowKey(tabletInfo.tableName, tabletInfo.endRowKey);
            Cell cell = new Cell(new Cell.Key(tabletInfo.tabletName), tabletInfo.getWriteBytes());
            ColumnValue columnValue = new ColumnValue();
            columnValue.setRowKey(rowKey);
            columnValue.setCellKey(cell.getKey());
            cell.getValue().copyToColumnValue(columnValue);
            columnValue.setDeleted(eachColumnValue.isDeleted());
            columnValue.setTimestamp(eachColumnValue.getTimestamp());
            
            MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
            ValueCollection.copyMapFileColumnValue(columnValue, mapFileColumnValue);
            writer.write(mapFileColumnValue);
            metaTablets.add(tabletInfo13);

            startRowKey = tabletInfo13.endRowKey;
            previousTableName = tabletInfo13.tableName;
          }          
        }
      } finally {
        writer.close();
      }

      //////////////////////////////////////////////////////////////////////
      //META Tablet
      for(TabletInfo13 eachMetaTablet: metaTablets) {
        mapFileColumnValues = getTabletInfos(eachMetaTablet);
        if(mapFileColumnValues.size() == 0) {
          LOG.warn("No USER TableInfo in META Tablet:" + eachMetaTablet);
          continue;
        }
        writer = new UpgradeFileWriter(eachMetaTablet.tabletName);
        try {
          Row.Key startRowKey = Row.Key.MIN_KEY;
          
          TabletInfo14 previousTablet = null;
//          Row.Key prevoiusEndRowKey = null;
//          String previousTableName = null;
//          String previousTabletName = null;
          
          for(MapFileColumnValue eachColumnValue: mapFileColumnValues) {
            if(eachColumnValue.isDeleted()) {
              writer.write(eachColumnValue);
            } else {
              TabletInfo13 tabletInfo13 = new TabletInfo13();
              tabletInfo13.readFields(eachColumnValue.getValue());
              TabletInfo14 tabletInfo14 = tabletInfo13.getTabletInfo();

              if(previousTablet != null) {
                if(!previousTablet.tableName.equals(tabletInfo13.tableName)) {
                  startRowKey = Row.Key.MIN_KEY;
                } else {
                  if(!previousTablet.tabletName.equals(tabletInfo13.tabletName) &&
                      !previousTablet.endRowKey.equals(tabletInfo13.endRowKey)) {
                    startRowKey = previousTablet.endRowKey;
                  }
                }
              } 
              
              tabletInfo14.startRowKey = startRowKey;
              System.out.println(">>>>" + tabletInfo14.tabletName + ">" + (startRowKey.equals(Row.Key.MIN_KEY) ? "MIN" : startRowKey) + "~" + 
                  (tabletInfo14.endRowKey.equals(Row.Key.MAX_KEY) ? "MAX" : tabletInfo14.endRowKey) + ">" + 
                  eachColumnValue.getTimestamp() + ">" + eachColumnValue.isDeleted() + ">");

              Row.Key rowKey = Tablet.generateMetaRowKey(tabletInfo14.tableName, tabletInfo14.endRowKey);
              Cell cell = new Cell(new Cell.Key(tabletInfo14.tabletName), tabletInfo14.getWriteBytes());
              ColumnValue columnValue = new ColumnValue();
              columnValue.setRowKey(rowKey);
              columnValue.setCellKey(cell.getKey());
              cell.getValue().copyToColumnValue(columnValue);
              columnValue.setDeleted(eachColumnValue.isDeleted());
              columnValue.setTimestamp(eachColumnValue.getTimestamp());
  
              MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
              ValueCollection.copyMapFileColumnValue(columnValue, mapFileColumnValue);
              writer.write(mapFileColumnValue);
              
//              prevoiusEndRowKey = tabletInfo13.endRowKey;
//              previousTabletName = tabletInfo13.tabletName;
//              previousTableName = tabletInfo13.tableName;
              
              previousTablet = tabletInfo14;
            }
          }
        } finally {
          writer.close();
        }
      }

      LOG.info("====== Modify ROOT TabletInfo in ZooKeeper: " + LockUtil.getZKPath(conf, Constants.ROOT_TABLET));
      TabletInfo14 newRootTablet = rootTablet.getTabletInfo();
      newRootTablet.startRowKey = Row.Key.MIN_KEY;
      
      try {
        zk.setData(LockUtil.getZKPath(conf, Constants.ROOT_TABLET), newRootTablet.getWriteBytes(), -1);
      } catch (KeeperException e) {
        LOG.error(e);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
    } catch (Exception e) {
      LOG.error("Error:" + e.getMessage(), e);
    } finally {
      LOG.info("====== End modifyMetaMapFile");
    }
  }

  private static List<MapFileColumnValue> getTabletInfos(TabletInfo13 tabletInfo)
      throws IOException {
    LOG.info("====== getTabletInfos from: " + tabletInfo.tabletName);
    List<MapFileColumnValue> tablets = new ArrayList<MapFileColumnValue>();

    CloudataFileSystem fs = CloudataFileSystem.get(conf);

    GPath[] columnPaths = fs.list(getTabletPath(conf, tabletInfo));

    if (columnPaths == null || columnPaths.length == 0) {
      LOG.warn("No ROOT TabletMapFile:" + getTabletPath(conf, tabletInfo));
      return tablets;
    }

    for (GPath eachColumnPath : columnPaths) {
      GPath[] mapFilePaths = fs.list(eachColumnPath);

      if (mapFilePaths == null || mapFilePaths.length == 0) {
        continue;
      }

      if(mapFilePaths.length > 1) {
        throw new IOException("MapFile not merged: " + eachColumnPath);
      }
      GPath dataPath = new GPath(mapFilePaths[0], TabletMapFile.DATA_FILE);
      tablets.addAll(readTabletMapFile(dataPath));
    }
    return tablets;
  }

  public static TabletInfo14 getRootTabletInfo14(CloudataConf conf, ZooKeeper zk) throws IOException {
    byte[] data = null;
    try {
      data = zk.getData(LockUtil.getZKPath(conf, Constants.ROOT_TABLET), false,
          null);
    } catch (NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (data == null) {
      LOG.warn(Constants.ROOT_TABLET + " is null"
          + ", so ROOT tablet info is also null");
      return null;
    }

    TabletInfo14 rootTabletInfo = new TabletInfo14();

    rootTabletInfo.readFields(data);
    rootTabletInfo.assignedHostName = null;

    data = null;
    try {
      data = zk.getData(LockUtil.getZKPath(conf, Constants.ROOT_TABLET_HOST),
          false, null);
    } catch (NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (data != null) {
      rootTabletInfo.assignedHostName = new String(data);
    }

    return rootTabletInfo;
  }
  
  public static TabletInfo13 getRootTabletInfo(CloudataConf conf, ZooKeeper zk)
      throws IOException {
    byte[] data = null;
    try {
      data = zk.getData(LockUtil.getZKPath(conf, Constants.ROOT_TABLET), false,
          null);
    } catch (NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (data == null) {
      LOG.warn(Constants.ROOT_TABLET + " is null"
          + ", so ROOT tablet info is also null");
      return null;
    }

    TabletInfo13 rootTabletInfo = new TabletInfo13();

    rootTabletInfo.readFields(data);
    rootTabletInfo.assignedHostName = null;

    data = null;
    try {
      data = zk.getData(LockUtil.getZKPath(conf, Constants.ROOT_TABLET_HOST),
          false, null);
    } catch (NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (data != null) {
      rootTabletInfo.assignedHostName = new String(data);
    }

    return rootTabletInfo;
  }

  private static GPath getTabletPath(CloudataConf conf,
      TabletInfo13 tabletInfo) {
    return new GPath(TableSchema.getTableDataPath(conf, tabletInfo.tableName)
        + "/" + tabletInfo.tabletName);
  }

  private static List<MapFileColumnValue> readTabletMapFile(GPath path)
      throws IOException {
    LOG.info("====== Read tablet info from: " + path);
    List<MapFileColumnValue> tablets = new ArrayList<MapFileColumnValue>();

    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    DataInputStream in = fs.openDataInputStream(path);

    while (true) {
      try {
        MapFileColumnValue columnValue = new MapFileColumnValue();
        columnValue.readFields(in);

        tablets.add(columnValue);
      } catch (EOFException e) {
        break;
      }
    }

    return tablets;
  }

  static class TabletInfo14 extends TabletInfo13 {
    protected Row.Key startRowKey;
    
    public TabletInfo14() {
      super();
      startRowKey = new Row.Key();
    }
    
    public void readFields(DataInput in) throws IOException {
      tableName = CWritableUtils.readString(in);
      tabletName = CWritableUtils.readString(in);
      
      startRowKey.readFields(in);
      endRowKey.readFields(in);
      assignedHostName = CWritableUtils.readString(in);
    }

    public void write(DataOutput out) throws IOException {
      CWritableUtils.writeString(out, tableName);
      CWritableUtils.writeString(out, tabletName);

      startRowKey.write(out);
      endRowKey.write(out);
      CWritableUtils.writeString(out, assignedHostName);
    }
    
    public TabletInfo13 getTabletInfo13() {
      TabletInfo13 tabletInfo = new TabletInfo13();
      tabletInfo.tableName = tableName;
      tabletInfo.tabletName = tabletName;
      tabletInfo.assignedHostName = assignedHostName;
      tabletInfo.endRowKey = endRowKey;
      return tabletInfo;
    }
  }
  
  static class TabletInfo13 implements CWritableComparable {
    protected String tableName;

    protected String tabletName;

    protected Row.Key endRowKey;

    String assignedHostName;

    public TabletInfo13() {
      endRowKey = new Row.Key();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TabletInfo13))
        return false;

      TabletInfo13 otherObj = (TabletInfo13) obj;
      return tabletName.equals(otherObj.tabletName);
    }

    @Override
    public int hashCode() {
      return tabletName.hashCode();
    }

    public void readFields(DataInput in) throws IOException {
      tableName = CWritableUtils.readString(in);
      tabletName = CWritableUtils.readString(in);

      endRowKey.readFields(in);
      assignedHostName = CWritableUtils.readString(in);
    }

    public void write(DataOutput out) throws IOException {
      CWritableUtils.writeString(out, tableName);
      CWritableUtils.writeString(out, tabletName);

      endRowKey.write(out);
      CWritableUtils.writeString(out, assignedHostName);
    }

    public void readFields(byte[] value) throws IOException {
      DataInputStream din = new DataInputStream(new ByteArrayInputStream(value));
      this.readFields(din);
    }

    public byte[] getWriteBytes() throws IOException {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream dout = new DataOutputStream(bout);
      this.write(dout);
      return bout.toByteArray();
    }

    public int compareTo(Object obj) {
      TabletInfo13 tabletInfo = (TabletInfo13) obj;
      return endRowKey.compareTo(tabletInfo.endRowKey);
    }
    
    public TabletInfo14 getTabletInfo() {
      TabletInfo14 tabletInfo = new TabletInfo14();
      tabletInfo.tableName = tableName;
      tabletInfo.tabletName = tabletName;
      tabletInfo.assignedHostName = assignedHostName;
      tabletInfo.endRowKey = endRowKey;
      return tabletInfo;
    }
  }
  
  static class UpgradeFileWriter {
    private DataOutputStream dataOut;
    private DataOutputStream indexOut;  
    
    private MapFileColumnValue lastColumnRecord;
    
    private int blockWriteBytes = 0;
    private long currentDataFilePos = 0;
    private long indexFileOffset = 0;
    
    private int indexBlockSize = conf.getInt("mapfile.indexBlockSize", 128) * 1024;
    
    private final int BLOCK_RECORD_COUNT = 100;
    
    private int blockWriteRecordCount = 0;
    
    private long sumDataLength = 0;
    private long sumIndexLength = 0;
    
    public UpgradeFileWriter(String fileName) throws IOException {
      dataOut = new DataOutputStream(new FileOutputStream(fileName + "_" + TabletMapFile.DATA_FILE));
      indexOut = new DataOutputStream(new FileOutputStream(fileName + "_" + TabletMapFile.IDX_FILE));
    }

    public int write(MapFileColumnValue mapFileColumnValue) throws IOException {
      mapFileColumnValue.write(dataOut);
      int writeSize = mapFileColumnValue.size();
      
      makeFileIndex(writeSize, mapFileColumnValue, indexBlockSize);
      
      lastColumnRecord = mapFileColumnValue;
      return writeSize;
    }

    private void makeFileIndex(int writeSize, MapFileColumnValue mapFileColumnValue, int checkSize) throws IOException {
      blockWriteBytes += writeSize;
      currentDataFilePos += writeSize;
      blockWriteRecordCount++;
      
      //누적된 writeBytes가 block size(64kb)보다 큰 경우 현재의 레코드를 index에 추가
      if(blockWriteBytes > checkSize || blockWriteRecordCount > BLOCK_RECORD_COUNT) {
        //index 파일에 index 레코드 추가
        MapFileIndexRecord mapFileIndexRecord = new MapFileIndexRecord(mapFileColumnValue.getRowKey(), mapFileColumnValue, indexFileOffset);
        mapFileIndexRecord.write(indexOut);
        
        sumIndexLength += mapFileIndexRecord.getMemorySize();
        
        blockWriteBytes = 0;
        blockWriteRecordCount = 0;
        indexFileOffset = currentDataFilePos;
      } 
    }
    
    public void close() throws IOException {
      if(dataOut == null) {
        return;
      }

      if(blockWriteBytes > 0) {
        makeFileIndex(0, lastColumnRecord, 0);
      }
      
      dataOut.flush();
      indexOut.flush();
            
      IOException exception = null;
      
      if(dataOut != null) {
        try {
          dataOut.close();
          dataOut = null;
        } catch (IOException e) { 
          LOG.error(e.getMessage() + ", sumDataLength=" + sumDataLength, e);
          exception = e;
        }
      }
      if(indexOut != null) {
        try {
          indexOut.close();
          indexOut = null;
        } catch (IOException e) { 
          LOG.error(e.getMessage() + ", sumIndexLength=" + sumIndexLength, e);
          exception = e;
        }
      }
//      closed = true;

      if(exception != null) {
        throw exception;
      }      
    }
  }  
}
