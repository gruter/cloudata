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
package org.cloudata.core.commitlog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.fs.DiskInfo;


/**
 * @author jindolk
 *
 */
public class ServerMonitorInfo implements CWritable {
  DiskInfo diskInfo;
  long logDirUsed;
  String logPath;
  
  long freeMemory;
  long totalMemory;
  long maxMemory;
  long serverStartTime;

  List<String> logFiles = new ArrayList<String>();
  
  long lastHeartbeatTime;

  public ServerMonitorInfo() {
    lastHeartbeatTime = System.currentTimeMillis();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    diskInfo = new DiskInfo();
    diskInfo.readFields(in);
    logDirUsed = in.readLong();
    logPath = CWritableUtils.readString(in);
    
    freeMemory = in.readLong();
    totalMemory = in.readLong();
    maxMemory = in.readLong();
    serverStartTime = in.readLong();
    
    lastHeartbeatTime = in.readLong();
    
    int fileCount = in.readInt();
    for(int i = 0; i < fileCount; i++) {
      logFiles.add(CWritableUtils.readString(in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if(diskInfo == null) {
      diskInfo = new DiskInfo();
    }
    diskInfo.write(out);
    out.writeLong(logDirUsed);
    CWritableUtils.writeString(out, logPath);
    
    out.writeLong(freeMemory);
    out.writeLong(totalMemory);
    out.writeLong(maxMemory);
    out.writeLong(serverStartTime);
    
    out.writeLong(lastHeartbeatTime);
    
    out.writeInt(logFiles.size());
    for(String eachLogFile: logFiles) {
      CWritableUtils.writeString(out, eachLogFile);
    }
  }

  public DiskInfo getDiskInfo() {
    return diskInfo;
  }

  public void setDiskInfo(DiskInfo diskInfo) {
    this.diskInfo = diskInfo;
  }

  public long getLogDirUsed() {
    return logDirUsed;
  }

  public void setLogDirUsed(long logDirUsed) {
    this.logDirUsed = logDirUsed;
  }

  public List<String> getLogFiles() {
    return logFiles;
  }

  public void setLogFiles(List<String> logFiles) {
    this.logFiles = logFiles;
  }

  public String getLogPath() {
    return logPath;
  }

  public void setLogPath(String logPath) {
    this.logPath = logPath;
  }

  public long getFreeMemory() {
    return freeMemory;
  }

  public void setFreeMemory(long freeMemory) {
    this.freeMemory = freeMemory;
  }

  public long getTotalMemory() {
    return totalMemory;
  }

  public void setTotalMemory(long totalMemory) {
    this.totalMemory = totalMemory;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public void setMaxMemory(long maxMemory) {
    this.maxMemory = maxMemory;
  }

  public long getServerStartTime() {
    return serverStartTime;
  }

  public void setServerStartTime(long serverStartTime) {
    this.serverStartTime = serverStartTime;
  }

  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  public void setLastHeartbeatTime(long lastHeartbeatTime) {
    this.lastHeartbeatTime = lastHeartbeatTime;
  }
}

