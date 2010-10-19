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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * Stores Tablet detail information
 *
 */
public class TabletReport implements CWritable {
  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###,###,###,###");
  private TabletInfo tabletInfo;
  String[] mapFilePaths;
  String[] runningActions;
  String[] waitingActions;
  int[] columnMapFileCount;
  long[] columnMapFileSize;
  long[] columnIndexSize;
  
  int memoryTabletSize;
  int[] memoryTabletDataCount;

  long serverMaxMemory;
  long serverFreeMemory;
  long serverTotalMemory;
  
  String[] commitLogServers;
  
  public String[] getCommitLogServers() {
    return commitLogServers;
  }

  public void setCommitLogServers(String[] commitLogServers) {
    this.commitLogServers = commitLogServers;
  }

  /**
   * @return the serverFreeMemory
   */
  public long getServerFreeMemory() {
    return serverFreeMemory;
  }

  public long[] getColumnIndexSize() {
    return columnIndexSize;
  }

  public void setColumnIndexSize(long[] columnIndexSize) {
    this.columnIndexSize = columnIndexSize;
  }

  /**
   * @param serverFreeMemory the serverFreeMemory to set
   */
  public void setServerFreeMemory(long serverFreeMemory) {
    this.serverFreeMemory = serverFreeMemory;
  }

  /**
   * @return the serverMaxMemory
   */
  public long getServerMaxMemory() {
    return serverMaxMemory;
  }

  /**
   * @param serverMaxMemory the serverMaxMemory to set
   */
  public void setServerMaxMemory(long serverMaxMemory) {
    this.serverMaxMemory = serverMaxMemory;
  }

  /**
   * @return the memoryTabletDataCount
   */
  public int[] getMemoryTabletDataCount() {
    return memoryTabletDataCount;
  }

  /**
   * @param memoryTabletDataCount the memoryTabletDataCount to set
   */
  public void setMemoryTabletDataCount(int[] memoryTabletDataCount) {
    this.memoryTabletDataCount = memoryTabletDataCount;
  }

  /**
   * @return the memoryTabletSize
   */
  public int getMemoryTabletSize() {
    return memoryTabletSize;
  }

  /**
   * @return the columnMapFileCount
   */
  public int[] getColumnMapFileCount() {
    return columnMapFileCount;
  }

  /**
   * @param columnMapFileCount the columnMapFileCount to set
   */
  public void setColumnMapFileCount(int[] columnMapFileCount) {
    this.columnMapFileCount = columnMapFileCount;
  }

  /**
   * @return the columnMapFileSize
   */
  public long[] getColumnMapFileSize() {
    return columnMapFileSize;
  }

  /**
   * @param columnMapFileSize the columnMapFileSize to set
   */
  public void setColumnMapFileSize(long[] columnMapFileSize) {
    this.columnMapFileSize = columnMapFileSize;
  }

  /**
   * @return the runningActions
   */
  public String[] getRunningActions() {
    return runningActions;
  }

  /**
   * @param runningActions the runningActions to set
   */
  public void setRunningActions(String[] runningActions) {
    this.runningActions = runningActions;
  }

  /**
   * @return the tabletInfo
   */
  public TabletInfo getTabletInfo() {
    return tabletInfo;
  }

  /**
   * @param tabletInfo the tabletInfo to set
   */
  public void setTabletInfo(TabletInfo tabletInfo) {
    this.tabletInfo = tabletInfo;
  }

  /**
   * @return the waitingActions
   */
  public String[] getWaitingActions() {
    return waitingActions;
  }

  /**
   * @param waitingActions the waitingActions to set
   */
  public void setWaitingActions(String[] waitingActions) {
    this.waitingActions = waitingActions;
  }

  public void readFields(DataInput in) throws IOException {
    tabletInfo = new TabletInfo();
    tabletInfo.readFields(in);
    runningActions = CWritableUtils.readStringArray(in);
    waitingActions = CWritableUtils.readStringArray(in);
    int lenth = in.readInt();
    columnMapFileCount = new int[lenth];
    for(int i = 0; i < columnMapFileCount.length; i++) {
      columnMapFileCount[i] = in.readInt();
    }

    lenth = in.readInt();
    columnMapFileSize = new long[lenth];
    for(int i = 0; i < columnMapFileSize.length; i++) {
      columnMapFileSize[i] = in.readLong();
    }
    
    memoryTabletSize = in.readInt();
    
    lenth = in.readInt();
    memoryTabletDataCount = new int[lenth];
    for(int i = 0; i < memoryTabletDataCount.length; i++) {
      memoryTabletDataCount[i] = in.readInt();
    }  
    
    lenth = in.readInt();
    columnIndexSize = new long[lenth];
    for(int i = 0; i < columnIndexSize.length; i++) {
      columnIndexSize[i] = in.readLong();
    }  
    
    serverMaxMemory = in.readLong();
    serverTotalMemory = in.readLong();
    serverFreeMemory = in.readLong();
    
    mapFilePaths = CWritableUtils.readStringArray(in);
    commitLogServers = CWritableUtils.readStringArray(in);
  }

  public void write(DataOutput out) throws IOException {
    tabletInfo.write(out);
    CWritableUtils.writeStringArray(out, runningActions);
    CWritableUtils.writeStringArray(out, waitingActions);
    
    out.writeInt(columnMapFileCount.length);
    for(int i = 0; i < columnMapFileCount.length; i++) {
      out.writeInt(columnMapFileCount[i]);
    }
    out.writeInt(columnMapFileSize.length);
    for(int i = 0; i < columnMapFileSize.length; i++) {
      out.writeLong(columnMapFileSize[i]);
    }
    
    out.writeInt(memoryTabletSize);

    out.writeInt(memoryTabletDataCount.length);
    for(int i = 0; i < memoryTabletDataCount.length; i++) {
      out.writeInt(memoryTabletDataCount[i]);
    }
    
    out.writeInt(columnIndexSize.length);
    for(int i = 0; i < columnIndexSize.length; i++) {
      out.writeLong(columnIndexSize[i]);
    }    
    
    out.writeLong(serverMaxMemory);
    out.writeLong(serverTotalMemory);
    out.writeLong(serverFreeMemory);
    
    CWritableUtils.writeStringArray(out, mapFilePaths);
    CWritableUtils.writeStringArray(out, commitLogServers);
  }

  public String[] getMapFilePaths() {
    return mapFilePaths;
  }

  public void setMapFilePaths(String[] mapFilePaths) {
    this.mapFilePaths = mapFilePaths;
  }

  /**
   * @param memoryTabletSize the memoryTabletSize to set
   */
  public void setMemoryTabletSize(int memoryTabletSize) {
    this.memoryTabletSize = memoryTabletSize;
  }

  public void print() {
    System.out.println("-----------------------------------------------------------------------------");
    System.out.println("TabletInfo  : " + tabletInfo);
    System.out.println("TabletServer: MaxMemory = " + serverMaxMemory/1024 + " KB, TotalMemory = " + serverTotalMemory/1024 + " KB, FreeMemory = " + serverFreeMemory/1024 + " KB");
    
    int maxCount = 0;
    for(int i = 0; i < memoryTabletDataCount.length; i++) {
      if(memoryTabletDataCount[i] > maxCount) {
        maxCount = memoryTabletDataCount[i];
      }
    }
    
    String commitLogServerList = "";
    for(int i = 0; i < commitLogServers.length; i++) {
      commitLogServerList += commitLogServers[i];
      if(i < commitLogServers.length - 1) {
        commitLogServerList += ",";  
      }
    }
    System.out.println("Memory      : " + memoryTabletSize + " bytes, " + maxCount + " rows, commitlog: " + commitLogServerList);

    maxCount = 0;
    int mapFileCount = 0;
    for(int i = 0; i < columnMapFileCount.length; i++) {
      if(columnMapFileCount[i] > maxCount) {
        maxCount = columnMapFileCount[i];
      }
      mapFileCount += columnMapFileCount[i];
    }
    
    System.out.println("Disk        : mapFileLength = " + sumMapFileSize() + " bytes, mapFileIndex = " + sumMapFileIndexSize() + " bytes, mapFileCount = " + mapFileCount + ", max column map count = " + maxCount);
    
    if(runningActions.length > 0)  {
      System.out.println("RunningActions");
      for(int i = 0; i < runningActions.length; i++) {
        System.out.println("\t" + runningActions[i]);
//        if(i >= 2) {
//          System.exit(0);
//        }
      }
    }
    
    if(waitingActions.length > 0)  {
      System.out.println("WaitingActions");
      for(int i = 0; i < waitingActions.length; i++) {
        System.out.println("\t" + waitingActions[i]);
//        if(i >= 2) {
//          System.exit(0);
//        }
      }
    }
    System.out.println("-----------------------------------------------------------------------------");
  }
  
  public void print(Writer writer) throws IOException {
    int maxCount = 0;
    for(int i = 0; i < memoryTabletDataCount.length; i++) {
      if(memoryTabletDataCount[i] > maxCount) {
        maxCount = memoryTabletDataCount[i];
      }
    }
    
    String commitLogServerList = "";
    if(commitLogServers.length == 0) {
      commitLogServerList = "empty";
    } else {
      for(int i = 0; i < commitLogServers.length; i++) {
        commitLogServerList += commitLogServers[i];
        if(i < commitLogServers.length - 1) {
          commitLogServerList += ",";  
        }
      }
    }
    
    writer.write("Memory: " + DECIMAL_FORMAT.format(memoryTabletSize) + " bytes," + DECIMAL_FORMAT.format(maxCount) + " rows, commitlog[" + commitLogServerList + "]<br>");

    maxCount = 0;
    int mapFileCount = 0;
    for(int i = 0; i < columnMapFileCount.length; i++) {
      if(columnMapFileCount[i] > maxCount) {
        maxCount = columnMapFileCount[i];
      }
      mapFileCount += columnMapFileCount[i];
    }
    
    writer.write("Disk: Data file = " + DECIMAL_FORMAT.format(sumMapFileSize()) + " bytes, index file = " + DECIMAL_FORMAT.format(sumMapFileIndexSize()) + 
        " bytes, data file count = " + mapFileCount + ", max column map count = " + maxCount + "<br>");
    
    if(runningActions.length > 0)  {
      writer.write("RunningActions:<br>");
      for(int i = 0; i < runningActions.length; i++) {
        writer.write("&nbsp;&nbsp;&nbsp;&nbsp;" + runningActions[i] + "<br>");
      }
    }
    
    if(waitingActions.length > 0)  {
      writer.write("WaitingActions:<br>");
      for(int i = 0; i < waitingActions.length; i++) {
        writer.write("&nbsp;&nbsp;&nbsp;&nbsp;" + waitingActions[i] + "<br>");
      }
    } 
  }

  /**
   * @return the serverTotalMemory
   */
  public long getServerTotalMemory() {
    return serverTotalMemory;
  }

  /**
   * @param serverTotalMemory the serverTotalMemory to set
   */
  public void setServerTotalMemory(long serverTotalMemory) {
    this.serverTotalMemory = serverTotalMemory;
  }

  public long sumMapFileSize() {
    long mapFileSize = 0;
    for(int i = 0; i < columnMapFileSize.length; i++) {
      mapFileSize += columnMapFileSize[i];
    }
    return mapFileSize;
  }
  
  public long sumMapFileIndexSize() {
    long mapFileIndexSize = 0;
    for(int i = 0; i < columnIndexSize.length; i++) {
      mapFileIndexSize += columnIndexSize[i];
    }
    return mapFileIndexSize;
  }
}
